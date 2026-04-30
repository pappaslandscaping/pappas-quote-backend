const express = require('express');
const { analyzePropertyMeasurement } = require('../services/measurements/analyzer');

module.exports = function createMeasurementRoutes({ pool, serverError, authenticateToken }) {
  const router = express.Router();

  async function loadProperty(propertyId) {
    const result = await pool.query('SELECT * FROM properties WHERE id = $1', [propertyId]);
    return result.rows[0] || null;
  }

  async function syncPropertyOverlay(propertyId, measurementPayload) {
    await pool.query(
      `UPDATE properties
       SET latitude = COALESCE($2, latitude),
           longitude = COALESCE($3, longitude),
           parcel_id = COALESCE($4, parcel_id),
           parcel_data = CASE WHEN $5::jsonb IS NULL THEN parcel_data ELSE $5::jsonb END,
           parcel_boundary_geojson = COALESCE($6::jsonb, parcel_boundary_geojson),
           measurement_summary = $7::jsonb,
           lot_size = COALESCE($8, lot_size),
           last_measured_at = CURRENT_TIMESTAMP
       WHERE id = $1`,
      [
        propertyId,
        measurementPayload.resolution.latitude,
        measurementPayload.resolution.longitude,
        measurementPayload.parcel?.parcelId || null,
        measurementPayload.parcel ? JSON.stringify(measurementPayload.parcel.rawData || {}) : null,
        measurementPayload.parcel?.boundaryGeojson ? JSON.stringify(measurementPayload.parcel.boundaryGeojson) : null,
        JSON.stringify({
          method: measurementPayload.method,
          engine: measurementPayload.engine || null,
          accuracy: measurementPayload.accuracy,
          fallback: measurementPayload.fallback,
          totalLot: measurementPayload.analysis.totalLot,
          lawnArea: measurementPayload.analysis.lawnArea,
          bedArea: measurementPayload.analysis.bedArea,
          hardscapeArea: measurementPayload.analysis.hardscapeArea,
          shrubCount: measurementPayload.analysis.shrubCount,
          measuredAt: new Date().toISOString(),
        }),
        measurementPayload.analysis.totalLot ? String(measurementPayload.analysis.totalLot) : null,
      ]
    );
  }

  async function createMeasurementRecord({ propertyId, requestId, payload }) {
    const versionResult = await pool.query(
      'SELECT COALESCE(MAX(version), 0) + 1 AS next_version FROM property_measurements WHERE property_id = $1',
      [propertyId]
    );
    const version = versionResult.rows[0]?.next_version || 1;

    const measurementResult = await pool.query(
      `INSERT INTO property_measurements
        (property_id, request_id, version, status, method, lot_area_sqft, confidence, summary, parcel_data, imagery_data)
       VALUES ($1, $2, $3, 'completed', $4, $5, $6::jsonb, $7::jsonb, $8::jsonb, $9::jsonb)
       RETURNING *`,
      [
        propertyId,
        requestId,
        version,
        payload.method,
        payload.analysis.totalLot || null,
        JSON.stringify(payload.analysis.confidence || {}),
        JSON.stringify({
          analysis: payload.analysis,
          engine: payload.engine || null,
          accuracy: payload.accuracy,
          fallback: payload.fallback,
          debug: payload.debug,
        }),
        JSON.stringify(payload.parcel || {}),
        JSON.stringify({
          imageUrl: payload.resolution.imageryUrl,
          address: payload.resolution.address,
          geocodeSource: payload.resolution.geocodeSource,
        }),
      ]
    );

    const measurement = measurementResult.rows[0];

    for (const feature of payload.features) {
      await pool.query(
        `INSERT INTO property_measurement_features
          (measurement_id, feature_key, feature_label, feature_group, geometry_type, unit, quantity, confidence, source, geometry_geojson, metadata)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'auto', $9::jsonb, $10::jsonb)`,
        [
          measurement.id,
          feature.feature_key,
          feature.feature_label,
          feature.feature_group,
          feature.geometry_type,
          feature.unit,
          feature.quantity,
          feature.confidence,
          feature.geometry_geojson ? JSON.stringify(feature.geometry_geojson) : null,
          JSON.stringify(feature.metadata || {}),
        ]
      );
    }

    return measurement;
  }

  async function hydrateMeasurement(measurementId) {
    const measurementResult = await pool.query('SELECT * FROM property_measurements WHERE id = $1', [measurementId]);
    const measurement = measurementResult.rows[0];
    if (!measurement) return null;

    const featuresResult = await pool.query(
      'SELECT * FROM property_measurement_features WHERE measurement_id = $1 ORDER BY id ASC',
      [measurementId]
    );

    return {
      ...measurement,
      features: featuresResult.rows,
    };
  }

  async function runMeasurement(propertyId, body, user, { resubmittedFromMeasurementId = null } = {}) {
    if (!Number.isInteger(propertyId)) {
      return { status: 400, body: { success: false, error: 'Invalid property id' } };
    }

    const property = await loadProperty(propertyId);
    if (!property) {
      return { status: 404, body: { success: false, error: 'Property not found' } };
    }

    const instructions = body?.instructions || {};
    const reportType = body?.report_type || 'landscaping';

    const requestResult = await pool.query(
      `INSERT INTO property_measurement_requests
        (property_id, requested_by_user_id, requested_by_name, status, request_type, report_type, instructions, source_snapshot)
       VALUES ($1, $2, $3, 'processing', 'automeasure', $4, $5::jsonb, $6::jsonb)
       RETURNING *`,
      [
        propertyId,
        user?.id || null,
        user?.name || user?.email || 'Unknown',
        reportType,
        JSON.stringify(instructions),
        JSON.stringify({
          property_name: property.property_name,
          street: property.street,
          city: property.city,
          state: property.state,
          zip: property.zip,
          resubmitted_from_measurement_id: resubmittedFromMeasurementId,
        }),
      ]
    );

    const requestRecord = requestResult.rows[0];

    try {
      const payload = await analyzePropertyMeasurement(property, {
        reportType,
        instructions,
        lotSize: body?.lot_size || property.lot_size || null,
      });

      const boundaryGeojson = payload.parcel?.boundaryGeojson || null;
      await pool.query(
        `UPDATE property_measurement_requests
         SET status = 'completed',
             boundary_source = $2,
             boundary_geojson = $3::jsonb,
             started_at = COALESCE(started_at, CURRENT_TIMESTAMP),
             completed_at = CURRENT_TIMESTAMP,
             updated_at = CURRENT_TIMESTAMP
         WHERE id = $1`,
        [
          requestRecord.id,
          boundaryGeojson ? 'parcel' : 'address',
          boundaryGeojson ? JSON.stringify(boundaryGeojson) : null,
        ]
      );

      if (resubmittedFromMeasurementId) {
        await pool.query(
          `UPDATE property_measurements
           SET status = 'superseded', updated_at = CURRENT_TIMESTAMP
           WHERE id = $1 AND property_id = $2`,
          [resubmittedFromMeasurementId, propertyId]
        );
      }

      const measurement = await createMeasurementRecord({
        propertyId,
        requestId: requestRecord.id,
        payload,
      });
      await syncPropertyOverlay(propertyId, payload);

      return {
        status: 200,
        body: {
          success: true,
          request: { ...requestRecord, status: 'completed' },
          measurement: await hydrateMeasurement(measurement.id),
        },
      };
    } catch (error) {
      await pool.query(
        `UPDATE property_measurement_requests
         SET status = 'failed',
             error_message = $2,
             completed_at = CURRENT_TIMESTAMP,
             updated_at = CURRENT_TIMESTAMP
         WHERE id = $1`,
        [requestRecord.id, error.message || 'Measurement failed']
      );
      throw error;
    }
  }

  router.post('/api/properties/:id/measurements', authenticateToken, async (req, res) => {
    try {
      const result = await runMeasurement(parseInt(req.params.id, 10), req.body, req.user);
      return res.status(result.status).json(result.body);
    } catch (error) {
      serverError(res, error, 'Error creating property measurement');
    }
  });

  router.get('/api/properties/:id/measurements', authenticateToken, async (req, res) => {
    try {
      const propertyId = parseInt(req.params.id, 10);
      if (!Number.isInteger(propertyId)) {
        return res.status(400).json({ success: false, error: 'Invalid property id' });
      }

      const requestsResult = await pool.query(
        'SELECT * FROM property_measurement_requests WHERE property_id = $1 ORDER BY created_at DESC',
        [propertyId]
      );
      const measurementsResult = await pool.query(
        'SELECT * FROM property_measurements WHERE property_id = $1 ORDER BY version DESC, created_at DESC',
        [propertyId]
      );

      res.json({
        success: true,
        requests: requestsResult.rows,
        measurements: measurementsResult.rows,
      });
    } catch (error) {
      serverError(res, error, 'Error listing property measurements');
    }
  });

  router.get('/api/properties/:id/measurements/:measurementId', authenticateToken, async (req, res) => {
    try {
      const propertyId = parseInt(req.params.id, 10);
      const measurementId = parseInt(req.params.measurementId, 10);
      if (!Number.isInteger(propertyId) || !Number.isInteger(measurementId)) {
        return res.status(400).json({ success: false, error: 'Invalid id' });
      }

      const measurement = await hydrateMeasurement(measurementId);
      if (!measurement || measurement.property_id !== propertyId) {
        return res.status(404).json({ success: false, error: 'Measurement not found' });
      }

      res.json({ success: true, measurement });
    } catch (error) {
      serverError(res, error, 'Error fetching property measurement');
    }
  });

  router.post('/api/properties/:id/measurements/:measurementId/resubmit', authenticateToken, async (req, res) => {
    try {
      const propertyId = parseInt(req.params.id, 10);
      const measurementId = parseInt(req.params.measurementId, 10);
      if (!Number.isInteger(propertyId) || !Number.isInteger(measurementId)) {
        return res.status(400).json({ success: false, error: 'Invalid id' });
      }

      const measurement = await hydrateMeasurement(measurementId);
      if (!measurement || measurement.property_id !== propertyId) {
        return res.status(404).json({ success: false, error: 'Measurement not found' });
      }

      const result = await runMeasurement(propertyId, req.body, req.user, {
        resubmittedFromMeasurementId: measurementId,
      });
      return res.status(result.status).json(result.body);
    } catch (error) {
      serverError(res, error, 'Error resubmitting property measurement');
    }
  });

  return router;
};
