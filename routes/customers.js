// ═══════════════════════════════════════════════════════════
// Customer & Property Routes — extracted from server.js
// Handles: customers CRUD, properties CRUD, imports,
//          search, dedup, pipeline, statement PDF
// ═══════════════════════════════════════════════════════════

const express = require('express');
const { validate, schemas } = require('../lib/validate');

module.exports = function createCustomerRoutes({ pool, serverError, authenticateToken, nextCustomerNumber, upload }) {
  const router = express.Router();

// id, property_name, country, state, street, street2, city, zip, tags, status, lot_size, notes, customer_id
// ═══════════════════════════════════════════════════════════

// GET /api/properties
router.get('/api/properties', async (req, res) => {
  try {
    const { status, city, search, sort, limit = 1000, offset = 0, customer_id } = req.query;

    let query = `
      SELECT p.*, c.name as customer_display_name, c.email as customer_email, c.phone as customer_phone,
        (SELECT MAX(job_date) FROM scheduled_jobs sj WHERE (sj.customer_id = p.customer_id OR LOWER(TRIM(sj.address)) = LOWER(TRIM(p.street))) AND sj.status IN ('completed','done') AND p.customer_id IS NOT NULL) as last_service_date,
        (SELECT MIN(job_date) FROM scheduled_jobs sj WHERE (sj.customer_id = p.customer_id OR LOWER(TRIM(sj.address)) = LOWER(TRIM(p.street))) AND sj.status IN ('pending','scheduled') AND sj.job_date >= CURRENT_DATE AND p.customer_id IS NOT NULL) as next_service_date
      FROM properties p
      LEFT JOIN customers c ON p.customer_id = c.id
      WHERE 1=1
    `;
    let countQuery = 'SELECT COUNT(*) FROM properties WHERE 1=1';
    const params = [];
    const countParams = [];
    let paramCount = 1;
    let countParamCount = 1;

    if (customer_id) {
      query += ` AND p.customer_id = $${paramCount}`;
      countQuery += ` AND customer_id = $${countParamCount}`;
      params.push(customer_id);
      countParams.push(customer_id);
      paramCount++;
      countParamCount++;
    }

    if (status) {
      query += ` AND LOWER(p.status) = LOWER($${paramCount})`;
      countQuery += ` AND LOWER(status) = LOWER($${countParamCount})`;
      params.push(status);
      countParams.push(status);
      paramCount++;
      countParamCount++;
    }
    
    if (city) {
      query += ` AND p.city ILIKE $${paramCount}`;
      countQuery += ` AND city ILIKE $${countParamCount}`;
      params.push(`%${city}%`);
      countParams.push(`%${city}%`);
      paramCount++;
      countParamCount++;
    }
    
    if (search) {
      query += ` AND (p.street ILIKE $${paramCount} OR p.property_name ILIKE $${paramCount} OR p.city ILIKE $${paramCount} OR c.name ILIKE $${paramCount})`;
      countQuery += ` AND (street ILIKE $${countParamCount} OR property_name ILIKE $${countParamCount} OR city ILIKE $${countParamCount})`;
      params.push(`%${search}%`);
      countParams.push(`%${search}%`);
      paramCount++;
      countParamCount++;
    }
    
    let orderBy = 'p.street ASC';
    switch (sort) {
      case 'address_asc': orderBy = 'p.street ASC'; break;
      case 'address_desc': orderBy = 'p.street DESC'; break;
      case 'customer_asc': orderBy = 'c.name ASC NULLS LAST'; break;
      case 'customer_desc': orderBy = 'c.name DESC NULLS LAST'; break;
      case 'city_asc': orderBy = 'p.city ASC'; break;
      case 'city_desc': orderBy = 'p.city DESC'; break;
      case 'newest': orderBy = 'p.id DESC'; break;
      case 'oldest': orderBy = 'p.id ASC'; break;
    }
    
    query += ` ORDER BY ${orderBy} LIMIT $${paramCount} OFFSET $${paramCount + 1}`;
    params.push(limit, offset);
    
    const [result, countResult] = await Promise.all([
      pool.query(query, params),
      pool.query(countQuery, countParams)
    ]);

    res.json({
      success: true,
      properties: result.rows,
      total: parseInt(countResult.rows[0].count),
      limit: parseInt(limit),
      offset: parseInt(offset)
    });
  } catch (error) {
    console.error('Error fetching properties:', error);
    serverError(res, error);
  }
});

// GET /api/properties/stats
router.get('/api/properties/stats', async (req, res) => {
  try {
    const [totalResult, activeResult, citiesResult, pricedResult] = await Promise.all([
      pool.query('SELECT COUNT(*) FROM properties'),
      pool.query("SELECT COUNT(*) FROM properties WHERE LOWER(status) = 'active'"),
      pool.query(`SELECT city, COUNT(*) as count FROM properties WHERE city IS NOT NULL AND city != '' GROUP BY city ORDER BY count DESC LIMIT 20`),
      pool.query(`SELECT COUNT(*) FROM properties WHERE lot_size IS NOT NULL AND lot_size != '' AND lot_size != '0'`)
    ]);
    
    res.json({
      success: true,
      stats: {
        total: parseInt(totalResult.rows[0].count),
        active: parseInt(activeResult.rows[0].count),
        inactive: parseInt(totalResult.rows[0].count) - parseInt(activeResult.rows[0].count),
        topCities: citiesResult.rows,
        citiesServed: citiesResult.rows.length,
        withPricing: parseInt(pricedResult.rows[0].count),
        revenue: {
          pricedProperties: parseInt(pricedResult.rows[0].count)
        }
      }
    });
  } catch (error) {
    console.error('Error fetching property stats:', error);
    serverError(res, error);
  }
});

// GET /api/properties/:id
router.get('/api/properties/:id', async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT p.*, c.name as customer_display_name, c.email as customer_email, c.phone as customer_phone,
        (SELECT MAX(job_date) FROM scheduled_jobs sj WHERE (sj.customer_id = p.customer_id OR LOWER(TRIM(sj.address)) = LOWER(TRIM(p.street))) AND sj.status IN ('completed','done') AND p.customer_id IS NOT NULL) as last_service_date,
        (SELECT MIN(job_date) FROM scheduled_jobs sj WHERE (sj.customer_id = p.customer_id OR LOWER(TRIM(sj.address)) = LOWER(TRIM(p.street))) AND sj.status IN ('pending','scheduled') AND sj.job_date >= CURRENT_DATE AND p.customer_id IS NOT NULL) as next_service_date
      FROM properties p
      LEFT JOIN customers c ON p.customer_id = c.id
      WHERE p.id = $1
    `, [req.params.id]);
    
    if (result.rows.length === 0) {
      return res.status(404).json({ success: false, error: 'Property not found' });
    }
    res.json({ success: true, property: result.rows[0] });
  } catch (error) {
    console.error('Error fetching property:', error);
    serverError(res, error);
  }
});

// POST /api/properties
router.post('/api/properties', async (req, res) => {
  try {
    const { property_name, street, street2, city, state, country, zip, lot_size, tags, status, notes, customer_id, stories, fence_type, assigned_crew, default_services, access_instructions, equipment_notes } = req.body;

    if (!street) {
      return res.status(400).json({ success: false, error: 'Street address is required' });
    }

    const result = await pool.query(`
      INSERT INTO properties (property_name, street, street2, city, state, country, zip, lot_size, tags, status, notes, customer_id, stories, fence_type, assigned_crew, default_services, access_instructions, equipment_notes)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
      RETURNING *
    `, [
      property_name || street, street, street2 || '', city || '', state || 'OH', country || 'US',
      zip || '', lot_size || '', tags || '', status || 'Active', notes || '', customer_id || null,
      stories || null, fence_type || null, assigned_crew || null, default_services || null, access_instructions || null, equipment_notes || null
    ]);

    res.json({ success: true, property: result.rows[0] });
  } catch (error) {
    console.error('Error creating property:', error);
    serverError(res, error);
  }
});

// PUT /api/properties/:id
router.put('/api/properties/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const { property_name, street, street2, city, state, country, zip, lot_size, tags, status, notes, customer_id,
            customer_name, address, postal_code, lawn_sqft, property_notes, mowing_price } = req.body;
    
    // Map new field names to your existing columns
    const actualStreet = street || address || '';
    const actualZip = zip || postal_code || '';
    const actualLotSize = lot_size || (lawn_sqft ? String(lawn_sqft) : '');
    const actualNotes = notes || property_notes || '';
    const actualPropertyName = property_name || customer_name || actualStreet;
    
    const result = await pool.query(`
      UPDATE properties SET
        property_name = $1, street = $2, street2 = $3, city = $4, state = $5,
        country = $6, zip = $7, lot_size = $8, tags = $9, status = $10, notes = $11, customer_id = $12,
        county_tax = $14, city_tax = $15, state_tax = $16
      WHERE id = $13
      RETURNING *
    `, [
      actualPropertyName, actualStreet, street2 || '', city || '', state || 'OH',
      country || 'US', actualZip, actualLotSize, tags || '', status || 'Active', actualNotes,
      customer_id || null, id,
      req.body.county_tax !== undefined ? req.body.county_tax : null,
      req.body.city_tax !== undefined ? req.body.city_tax : null,
      req.body.state_tax !== undefined ? req.body.state_tax : null
    ]);
    
    if (result.rows.length === 0) {
      return res.status(404).json({ success: false, error: 'Property not found' });
    }
    res.json({ success: true, property: result.rows[0] });
  } catch (error) {
    console.error('Error updating property:', error);
    serverError(res, error);
  }
});

// PATCH /api/properties/:id
router.patch('/api/properties/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const updates = req.body;
    
    // Map field names
    const fieldMap = {
      'address': 'street', 'postal_code': 'zip', 'lawn_sqft': 'lot_size',
      'property_notes': 'notes', 'customer_name': 'property_name'
    };
    
    const allowedFields = ['property_name', 'street', 'street2', 'city', 'state', 'country', 'zip', 'lot_size', 'tags', 'status', 'notes', 'customer_id', 'county_tax', 'city_tax', 'state_tax', 'stories', 'fence_type', 'assigned_crew', 'default_services', 'access_instructions', 'equipment_notes', 'photos'];
    
    const setClause = [];
    const values = [];
    let paramCount = 1;
    
    Object.keys(updates).forEach(key => {
      const dbField = fieldMap[key] || key;
      if (allowedFields.includes(dbField)) {
        let value = updates[key];
        if (dbField === 'lot_size' && typeof value === 'number') value = String(value);
        setClause.push(`${dbField} = $${paramCount}`);
        values.push(value);
        paramCount++;
      }
    });
    
    if (setClause.length === 0) {
      return res.status(400).json({ success: false, error: 'No valid fields to update' });
    }
    
    values.push(id);
    const query = `UPDATE properties SET ${setClause.join(', ')} WHERE id = $${paramCount} RETURNING *`;
    const result = await pool.query(query, values);
    
    if (result.rows.length === 0) {
      return res.status(404).json({ success: false, error: 'Property not found' });
    }
    res.json({ success: true, property: result.rows[0] });
  } catch (error) {
    console.error('Error updating property:', error);
    serverError(res, error);
  }
});

// DELETE /api/properties/:id
router.delete('/api/properties/:id', async (req, res) => {
  try {
    const result = await pool.query('DELETE FROM properties WHERE id = $1 RETURNING *', [req.params.id]);
    if (result.rows.length === 0) {
      return res.status(404).json({ success: false, error: 'Property not found' });
    }
    res.json({ success: true, deleted: result.rows[0] });
  } catch (error) {
    console.error('Error deleting property:', error);
    serverError(res, error);
  }
});

// POST /api/properties/:id/photos - Upload photos (base64 in JSONB)
router.post('/api/properties/:id/photos', upload.array('photos', 10), async (req, res) => {
  try {
    const { id } = req.params;
    const prop = await pool.query('SELECT photos FROM properties WHERE id = $1', [id]);
    if (prop.rows.length === 0) return res.status(404).json({ success: false, error: 'Property not found' });

    const existing = prop.rows[0].photos || [];
    const newPhotos = [];
    for (const file of (req.files || [])) {
      const b64 = file.buffer.toString('base64');
      const dataUrl = `data:${file.mimetype};base64,${b64}`;
      newPhotos.push({ url: dataUrl, name: file.originalname, uploaded: new Date().toISOString() });
    }
    const allPhotos = [...existing, ...newPhotos];
    await pool.query('UPDATE properties SET photos = $1 WHERE id = $2', [JSON.stringify(allPhotos), id]);
    res.json({ success: true, photos: allPhotos });
  } catch (error) {
    console.error('Error uploading property photos:', error);
    serverError(res, error);
  }
});

// DELETE /api/properties/:id/photos/:index - Remove a photo
router.delete('/api/properties/:id/photos/:index', async (req, res) => {
  try {
    const { id, index } = req.params;
    const prop = await pool.query('SELECT photos FROM properties WHERE id = $1', [id]);
    if (prop.rows.length === 0) return res.status(404).json({ success: false, error: 'Property not found' });
    const photos = prop.rows[0].photos || [];
    photos.splice(parseInt(index), 1);
    await pool.query('UPDATE properties SET photos = $1 WHERE id = $2', [JSON.stringify(photos), id]);
    res.json({ success: true, photos });
  } catch (error) { serverError(res, error); }
});

// GET /api/properties/:id/service-history
router.get('/api/properties/:id/service-history', async (req, res) => {
  try {
    const prop = await pool.query('SELECT * FROM properties WHERE id = $1', [req.params.id]);
    if (prop.rows.length === 0) return res.status(404).json({ success: false, error: 'Property not found' });
    const p = prop.rows[0];
    const result = await pool.query(`
      SELECT * FROM scheduled_jobs
      WHERE (customer_id = $1 OR LOWER(TRIM(address)) = LOWER(TRIM($2)))
      ORDER BY job_date DESC LIMIT 50
    `, [p.customer_id, p.street]);
    res.json({ success: true, jobs: result.rows });
  } catch (error) {
    console.error('Error fetching property service history:', error);
    serverError(res, error);
  }
});

// POST /api/import-properties - Using YOUR column names
router.post('/api/import-properties', upload.single('csvfile'), async (req, res) => {
  try {
    if (!req.file) {
      return res.status(400).json({ success: false, error: 'No CSV file uploaded' });
    }
    
    const csvContent = req.file.buffer.toString('utf-8');
    const lines = csvContent.split('\n');
    const rawHeaders = parseCSVLine(lines[0]);
    const headers = rawHeaders.map(h => h.trim().toLowerCase().replace(/\s+/g, '_'));
    
    console.log('📋 CSV Headers:', headers);
    
    const properties = [];
    for (let i = 1; i < lines.length; i++) {
      const line = lines[i].trim();
      if (!line) continue;
      try {
        const values = parseCSVLine(line);
        if (values.length >= headers.length - 2) {
          const property = {};
          headers.forEach((h, idx) => {
            property[h] = (values[idx] || '').trim().replace(/^\t+/, '');
          });
          properties.push(property);
        }
      } catch (e) { console.log(`Skip line ${i + 1}`); }
    }
    
    console.log(`📊 Found ${properties.length} properties`);
    
    let imported = 0, updated = 0, skipped = 0;
    const errors = [];
    
    for (const prop of properties) {
      try {
        const street = prop['street'] || '';
        if (!street || street.toLowerCase() === 'primary') { skipped++; continue; }
        
        const propertyName = prop['property_name'] || street;
        const city = prop['city'] || '';
        const state = prop['state'] || 'OH';
        const country = prop['country'] || 'US';
        const zip = prop['zip'] || '';
        const street2 = prop['street2'] || '';
        const lotSize = prop['lot_size'] || '0';
        const status = prop['status'] || 'Active';
        const tags = prop['tags'] || '';
        const notes = prop['notes'] || '';
        
        const existing = await pool.query('SELECT id FROM properties WHERE street ILIKE $1', [street]);
        
        if (existing.rows.length > 0) {
          await pool.query(`
            UPDATE properties SET
              property_name = COALESCE(NULLIF($1, ''), property_name),
              city = COALESCE(NULLIF($2, ''), city),
              state = COALESCE(NULLIF($3, ''), state),
              country = COALESCE(NULLIF($4, ''), country),
              zip = COALESCE(NULLIF($5, ''), zip),
              street2 = COALESCE(NULLIF($6, ''), street2),
              lot_size = COALESCE(NULLIF($7, ''), lot_size),
              tags = COALESCE(NULLIF($8, ''), tags),
              status = COALESCE(NULLIF($9, ''), status),
              notes = COALESCE(NULLIF($10, ''), notes)
            WHERE id = $11
          `, [propertyName, city, state, country, zip, street2, lotSize, tags, status, notes, existing.rows[0].id]);
          updated++;
        } else {
          await pool.query(`
            INSERT INTO properties (property_name, street, street2, city, state, country, zip, lot_size, tags, status, notes)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
          `, [propertyName, street, street2, city, state, country, zip, lotSize, tags, status, notes]);
          imported++;
        }
      } catch (error) {
        console.error('Import error:', error.message);
        errors.push({ address: prop['street'], error: error.message });
        skipped++;
      }
    }
    
    console.log(`✅ Import: ${imported}, Updated: ${updated}, Skipped: ${skipped}`);
    
    res.json({
      success: true,
      message: 'Properties import completed',
      stats: { total: properties.length, imported, updated, skipped, errors: errors.slice(0, 10) }
    });
  } catch (error) {
    console.error('Import failed:', error);
    serverError(res, error);
  }
});

router.get('/api/customers', async (req, res) => {
  try {
    const { status, city, search, sort, type, limit = 1000, offset = 0 } = req.query;
    let query = 'SELECT * FROM customers WHERE 1=1';
    let countQuery = 'SELECT COUNT(*) FROM customers WHERE 1=1';
    const params = [], countParams = [];
    let p = 1, cp = 1;

    if (type) { query += ` AND customer_type = $${p++}`; countQuery += ` AND customer_type = $${cp++}`; params.push(type); countParams.push(type); }
    if (status) { query += ` AND status = $${p++}`; countQuery += ` AND status = $${cp++}`; params.push(status); countParams.push(status); }
    if (city) { query += ` AND city ILIKE $${p++}`; countQuery += ` AND city ILIKE $${cp++}`; params.push(`%${city}%`); countParams.push(`%${city}%`); }
    if (search) { query += ` AND (name ILIKE $${p} OR first_name ILIKE $${p} OR last_name ILIKE $${p} OR email ILIKE $${p} OR street ILIKE $${p})`; countQuery += ` AND (name ILIKE $${cp} OR first_name ILIKE $${cp} OR last_name ILIKE $${cp} OR email ILIKE $${cp})`; params.push(`%${search}%`); countParams.push(`%${search}%`); p++; cp++; }
    
    let orderBy = 'name ASC';
    if (sort === 'name_desc') orderBy = 'name DESC';
    else if (sort === 'newest') orderBy = 'created_at DESC';
    else if (sort === 'city_asc') orderBy = 'city ASC';
    
    query += ` ORDER BY ${orderBy} LIMIT $${p++} OFFSET $${p}`;
    params.push(limit, offset);
    
    const [result, countResult] = await Promise.all([
      pool.query(query, params),
      pool.query(countQuery, countParams)
    ]);
    res.json({ success: true, customers: result.rows, total: parseInt(countResult.rows[0].count) });
  } catch (error) { serverError(res, error); }
});

router.get('/api/customers/stats', async (req, res) => {
  try {
    const [total, active, cities, recent, previous] = await Promise.all([
      pool.query('SELECT COUNT(*) FROM customers'),
      pool.query("SELECT COUNT(*) FROM customers WHERE LOWER(status) = 'active'"),
      pool.query('SELECT city, COUNT(*) as count FROM customers WHERE city IS NOT NULL GROUP BY city ORDER BY count DESC LIMIT 10'),
      pool.query("SELECT COUNT(*) FROM customers WHERE created_at >= NOW() - INTERVAL '30 days'"),
      pool.query("SELECT COUNT(*) FROM customers WHERE created_at >= NOW() - INTERVAL '60 days' AND created_at < NOW() - INTERVAL '30 days'")
    ]);
    const recentCount = parseInt(recent.rows[0].count);
    const prevCount = parseInt(previous.rows[0].count);
    let trendPct = 0;
    if (prevCount > 0) trendPct = Math.round(((recentCount - prevCount) / prevCount) * 100);
    else if (recentCount > 0) trendPct = 100;
    const inactive = parseInt(total.rows[0].count) - parseInt(active.rows[0].count);
    res.json({ success: true, stats: { total: parseInt(total.rows[0].count), active: parseInt(active.rows[0].count), inactive, topCities: cities.rows, trend: { recent: recentCount, previous: prevCount, pct: trendPct } } });
  } catch (error) { serverError(res, error); }
});

// GET /api/customers/pipeline-stats - Lead vs Customer pipeline metrics
router.get('/api/customers/pipeline-stats', async (req, res) => {
  try {
    const [leads, customers, newLeads, converted] = await Promise.all([
      pool.query("SELECT COUNT(*) FROM customers WHERE customer_type = 'lead'"),
      pool.query("SELECT COUNT(*) FROM customers WHERE customer_type = 'customer' OR customer_type IS NULL"),
      pool.query("SELECT COUNT(*) FROM customers WHERE customer_type = 'lead' AND created_at >= NOW() - INTERVAL '30 days'"),
      pool.query("SELECT COUNT(*) FROM customers WHERE customer_type = 'customer' AND created_at >= NOW() - INTERVAL '30 days'")
    ]);
    const totalLeads = parseInt(leads.rows[0].count);
    const totalCustomers = parseInt(customers.rows[0].count);
    const conversionRate = totalLeads + totalCustomers > 0 ? Math.round((totalCustomers / (totalLeads + totalCustomers)) * 100) : 0;
    res.json({ success: true, stats: {
      totalLeads, totalCustomers,
      newLeadsThisMonth: parseInt(newLeads.rows[0].count),
      convertedThisMonth: parseInt(converted.rows[0].count),
      conversionRate
    }});
  } catch (error) { serverError(res, error); }
});

// GET /api/customers/search - Search customers by name for auto-fill
// IMPORTANT: This must come BEFORE /api/customers/:id to avoid :id matching "search"
router.get('/api/customers/search', async (req, res) => {
  try {
    const query = req.query.name || req.query.q || req.query.search || '';
    if (!query || query.length < 2) {
      return res.json({ success: true, customers: [] });
    }

    const result = await pool.query(
      `SELECT id, COALESCE(name, TRIM(COALESCE(first_name,'') || ' ' || COALESCE(last_name,'')), 'Unknown') as name, email, phone, mobile, street, city, state, postal_code
       FROM customers
       WHERE LOWER(COALESCE(name, '')) LIKE LOWER($1)
          OR LOWER(COALESCE(first_name, '')) LIKE LOWER($1)
          OR LOWER(COALESCE(last_name, '')) LIKE LOWER($1)
          OR LOWER(COALESCE(first_name,'') || ' ' || COALESCE(last_name,'')) LIKE LOWER($1)
          OR LOWER(COALESCE(email, '')) LIKE LOWER($1)
          OR COALESCE(phone, '') LIKE $1
       ORDER BY COALESCE(name, first_name, last_name, '')
       LIMIT 10`,
      [`%${query}%`]
    );

    res.json({ success: true, customers: result.rows });
  } catch (error) {
    console.error('Error searching customers:', error);
    serverError(res, error);
  }
});

router.get('/api/customers/:id', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM customers WHERE id = $1', [req.params.id]);
    if (result.rows.length === 0) return res.status(404).json({ success: false, error: 'Not found' });
    res.json({ success: true, customer: result.rows[0] });
  } catch (error) { serverError(res, error); }
});

// POST /api/customers/deduplicate - Merge duplicate customers (same name)
router.post('/api/customers/deduplicate', async (req, res) => {
  try {
    // Find groups of customers with the same name (case-insensitive, collapse whitespace, strip emails/QB IDs)
    const dupes = await pool.query(`
      SELECT norm_name, array_agg(id ORDER BY
        CASE WHEN qb_id IS NOT NULL THEN 0 ELSE 1 END, id ASC
      ) as ids, COUNT(*) as cnt
      FROM (
        SELECT id, qb_id,
          TRIM(LOWER(
            regexp_replace(
              regexp_replace(
                regexp_replace(name, '\\s*\\([^)]*@[^)]*\\)', '', 'g'),
              '\\s+#\\d+.*$', ''),
            '\\s+', ' ', 'g')
          )) as norm_name
        FROM customers
        WHERE name IS NOT NULL AND TRIM(name) != ''
      ) sub
      WHERE norm_name != ''
      GROUP BY norm_name
      HAVING COUNT(*) > 1
    `);

    let merged = 0;
    let deleted = 0;

    for (const row of dupes.rows) {
      const ids = row.ids; // First id = keeper (has qb_id or lowest id)
      const keepId = ids[0];
      const removeIds = ids.slice(1);

      // Merge any fields the keeper is missing from the duplicates
      const keeper = await pool.query('SELECT * FROM customers WHERE id = $1', [keepId]);
      const k = keeper.rows[0];

      for (const dupId of removeIds) {
        const dup = await pool.query('SELECT * FROM customers WHERE id = $1', [dupId]);
        const d = dup.rows[0];
        if (!d) continue;

        // Fill in any blank fields on keeper from duplicate
        const updates = [];
        const vals = [];
        let p = 1;
        const fields = ['email','phone','mobile','street','street2','city','state','postal_code','qb_id','notes','customer_company_name'];
        for (const f of fields) {
          if (!k[f] && d[f]) { updates.push(`${f}=$${p++}`); vals.push(d[f]); }
        }
        // Merge tags: combine from both records, deduplicate
        const kTags = (k.tags || '').split(',').map(t => t.trim()).filter(Boolean);
        const dTags = (d.tags || '').split(',').map(t => t.trim()).filter(Boolean);
        const mergedTags = [...new Set([...kTags, ...dTags])].join(', ');
        if (mergedTags && mergedTags !== (k.tags || '')) {
          updates.push(`tags=$${p++}`);
          vals.push(mergedTags);
          k.tags = mergedTags;
        }
        if (updates.length > 0) {
          vals.push(keepId);
          await pool.query(`UPDATE customers SET ${updates.join(',')} WHERE id=$${p}`, vals);
          k[fields.find((f,i) => updates[i])] = vals[0]; // keep local state updated
        }

        // Re-point all FK references from dupId → keepId
        await Promise.all([
          pool.query('UPDATE invoices SET customer_id=$1 WHERE customer_id=$2', [keepId, dupId]),
          pool.query('UPDATE properties SET customer_id=$1 WHERE customer_id=$2', [keepId, dupId]),
          pool.query('UPDATE scheduled_jobs SET customer_id=$1 WHERE customer_id=$2', [keepId, dupId]),
          pool.query('UPDATE messages SET customer_id=$1 WHERE customer_id=$2', [keepId, dupId])
        ]);

        // Delete the duplicate
        await pool.query('DELETE FROM customers WHERE id=$1', [dupId]);
        deleted++;
      }
      merged++;
    }

    // --- Deduplicate invoices by qb_invoice_id (keep lowest id) ---
    let invoicesDuped = 0;
    const dupInvoices = await pool.query(`
      SELECT qb_invoice_id, array_agg(id ORDER BY id ASC) as ids
      FROM invoices
      WHERE qb_invoice_id IS NOT NULL
      GROUP BY qb_invoice_id HAVING COUNT(*) > 1
    `);
    for (const row of dupInvoices.rows) {
      const [keepId, ...removeIds] = row.ids;
      for (const rid of removeIds) {
        await pool.query('DELETE FROM invoices WHERE id=$1', [rid]);
        invoicesDuped++;
      }
    }

    // Also deduplicate invoices by invoice_number (keep the one with qb_invoice_id, else lowest id)
    const dupInvByNum = await pool.query(`
      SELECT invoice_number, array_agg(id ORDER BY
        CASE WHEN qb_invoice_id IS NOT NULL THEN 0 ELSE 1 END, id ASC
      ) as ids
      FROM invoices
      WHERE invoice_number IS NOT NULL
      GROUP BY invoice_number HAVING COUNT(*) > 1
    `);
    for (const row of dupInvByNum.rows) {
      const [keepId, ...removeIds] = row.ids;
      for (const rid of removeIds) {
        await pool.query('DELETE FROM invoices WHERE id=$1', [rid]);
        invoicesDuped++;
      }
    }

    // --- Deduplicate expenses by qb_id (keep lowest id) ---
    let expensesDuped = 0;
    const dupExpenses = await pool.query(`
      SELECT qb_id, array_agg(id ORDER BY id ASC) as ids
      FROM expenses
      WHERE qb_id IS NOT NULL
      GROUP BY qb_id HAVING COUNT(*) > 1
    `);
    for (const row of dupExpenses.rows) {
      const [keepId, ...removeIds] = row.ids;
      for (const rid of removeIds) {
        await pool.query('DELETE FROM expenses WHERE id=$1', [rid]);
        expensesDuped++;
      }
    }

    res.json({
      success: true,
      customers: { groupsMerged: merged, duplicatesRemoved: deleted },
      invoices: { duplicatesRemoved: invoicesDuped },
      expenses: { duplicatesRemoved: expensesDuped }
    });
  } catch (e) {
    console.error('Dedup error:', e);
    serverError(res, e);
  }
});

// POST /api/customers/clean-names - Strip QB ID junk and embedded emails from customer names
router.post('/api/customers/clean-names', async (req, res) => {
  try {
    let totalCleaned = 0;

    // Step 1: Strip " #digits ..." suffix  e.g. "John Smith #1053406 John Smith" → "John Smith"
    const r1 = await pool.query(`
      UPDATE customers
      SET name = TRIM(regexp_replace(name, '\\s+#\\d+.*$', ''))
      WHERE name ~ '\\s+#\\d+'
    `);
    totalCleaned += r1.rowCount;

    // Step 2: Strip embedded emails in parens  e.g. "Ada VanMoulken (ada@gmail.com)" → "Ada VanMoulken"
    const r2 = await pool.query(`
      UPDATE customers
      SET name = TRIM(regexp_replace(name, '\\s*\\([^)]*@[^)]*\\)', '', 'g'))
      WHERE name ~ '\\([^)]*@[^)]*\\)'
    `);
    totalCleaned += r2.rowCount;

    // Step 3: Collapse multiple spaces left over
    await pool.query(`
      UPDATE customers
      SET name = TRIM(regexp_replace(name, '\\s+', ' ', 'g'))
      WHERE name ~ '\\s{2,}'
    `);

    res.json({ success: true, cleaned: totalCleaned });
  } catch (e) {
    console.error('Clean names error:', e);
    serverError(res, e);
  }
});

// POST /api/customers - Create new customer (from Zapier/CopilotCRM sync)
router.post('/api/customers', validate(schemas.createCustomer), async (req, res) => {
  try {
    const {
      customer_number, name, firstName, first_name, lastName, last_name,
      email, phone, mobile, fax, street, street2, city, state,
      postal_code, zip, postalCode, country, type, tags, notes, status
    } = req.body;

    // Handle name variations from CopilotCRM
    const finalFirstName = firstName || first_name || null;
    const finalLastName = lastName || last_name || null;
    const finalName = name || (finalFirstName && finalLastName ? `${finalFirstName} ${finalLastName}` : finalFirstName || finalLastName || 'Unknown');
    const finalPostalCode = postal_code || zip || postalCode || null;
    const finalStatus = status || 'Active';

    // Check for duplicates by email or customer_number
    if (email) {
      const existing = await pool.query('SELECT id FROM customers WHERE email = $1', [email]);
      if (existing.rows.length > 0) {
        console.log('⚠️ Customer already exists with email:', email);
        return res.json({ success: true, message: 'Customer already exists', customer_id: existing.rows[0].id });
      }
    }

    const result = await pool.query(`
      INSERT INTO customers (
        customer_number, name, email, status, customer_type,
        street, city, state, postal_code, phone, mobile,
        first_name, last_name, tags, notes, created_at, updated_at
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
      RETURNING *
    `, [
      customer_number || await nextCustomerNumber(),
      finalName,
      email || null,
      finalStatus,
      type || 'customer',
      street || null,
      city || null,
      state || null,
      finalPostalCode,
      phone || null,
      mobile || null,
      finalFirstName,
      finalLastName,
      tags || null,
      notes || null
    ]);

    console.log('👤 Customer created from Zapier:', finalName, email);
    res.json({ success: true, customer: result.rows[0] });
  } catch (error) {
    console.error('Error creating customer:', error);
    serverError(res, error);
  }
});

router.get('/api/customers/:id/properties', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM properties WHERE customer_id = $1', [req.params.id]);
    res.json({ success: true, properties: result.rows });
  } catch (error) { serverError(res, error); }
});

router.patch('/api/customers/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const allowed = ['name', 'first_name', 'last_name', 'status', 'email', 'phone', 'mobile', 'street', 'street2', 'city', 'state', 'postal_code', 'tags', 'notes', 'customer_type', 'customer_company_name', 'tax_exempt'];
    const sets = [], vals = [];
    let p = 1;
    // Map frontend field names to DB column names
    const fieldMap = { type: 'customer_type', company_name: 'customer_company_name' };
    Object.keys(req.body).forEach(k => {
      const dbCol = fieldMap[k] || k;
      if (allowed.includes(dbCol)) {
        let val = req.body[k];
        if (dbCol === 'tax_exempt') val = val === true || val === 'true';
        sets.push(`${dbCol} = $${p++}`);
        vals.push(val);
      }
    });
    if (sets.length === 0) return res.status(400).json({ success: false, error: 'No fields' });
    sets.push('updated_at = CURRENT_TIMESTAMP');
    vals.push(id);
    const result = await pool.query(`UPDATE customers SET ${sets.join(', ')} WHERE id = $${p} RETURNING *`, vals);
    res.json({ success: true, customer: result.rows[0] });
  } catch (error) { serverError(res, error); }
});

router.delete('/api/customers/:id', async (req, res) => {
  try {
    const result = await pool.query('DELETE FROM customers WHERE id = $1 RETURNING *', [req.params.id]);
    res.json({ success: true, deleted: result.rows[0] });
  } catch (error) { serverError(res, error); }
});

// GET /api/customers/:id/quotes - Get all quotes for a customer
router.get('/api/customers/:id/quotes', async (req, res) => {
  try {
    // First get the customer's email
    const customerResult = await pool.query('SELECT email FROM customers WHERE id = $1', [req.params.id]);
    if (customerResult.rows.length === 0) {
      return res.status(404).json({ success: false, error: 'Customer not found' });
    }
    const customerEmail = customerResult.rows[0].email;
    
    // Get all quotes for this customer by email
    const quotesResult = await pool.query(
      `SELECT id, quote_number, customer_name, customer_email, customer_address,
              services, subtotal, tax_amount, total, monthly_payment, quote_type,
              status, created_at, sent_at, viewed_at, contract_signed_at
       FROM sent_quotes
       WHERE LOWER(customer_email) = LOWER($1)
       ORDER BY created_at DESC`,
      [customerEmail]
    );
    
    res.json({ success: true, quotes: quotesResult.rows });
  } catch (error) { 
    console.error('Error fetching customer quotes:', error);
    serverError(res, error); 
  }
});

// GET /api/customers/:id/jobs - Get all scheduled jobs for a customer
router.get('/api/customers/:id/jobs', async (req, res) => {
  try {
    const customerResult = await pool.query('SELECT name, first_name, last_name FROM customers WHERE id = $1', [req.params.id]);
    if (customerResult.rows.length === 0) return res.status(404).json({ success: false, error: 'Customer not found' });
    const c = customerResult.rows[0];
    const customerName = c.name || ((c.first_name || '') + ' ' + (c.last_name || '')).trim();

    const jobsResult = await pool.query(
      `SELECT id, job_date, job_date AS scheduled_date, customer_name, service_type, service_price, address, status, completed_at, crew_assigned
       FROM scheduled_jobs
       WHERE customer_id = $1
         OR LOWER(customer_name) = LOWER($2)
         OR LOWER(customer_name) LIKE LOWER($2) || ' %'
       ORDER BY job_date DESC LIMIT 50`,
      [req.params.id, customerName]
    );
    res.json({ success: true, jobs: jobsResult.rows });
  } catch (error) {
    console.error('Error fetching customer jobs:', error);
    serverError(res, error);
  }
});

// GET /api/customers/:id/invoices - Get all invoices for a customer
router.get('/api/customers/:id/invoices', async (req, res) => {
  try {
    const customerResult = await pool.query('SELECT name, first_name, last_name, email FROM customers WHERE id = $1', [req.params.id]);
    if (customerResult.rows.length === 0) return res.status(404).json({ success: false, error: 'Customer not found' });
    const c = customerResult.rows[0];
    const customerName = c.name || ((c.first_name || '') + ' ' + (c.last_name || '')).trim();

    const invoicesResult = await pool.query(
      `SELECT id, invoice_number, customer_name, customer_email, total, status, due_date, paid_at, created_at
       FROM invoices
       WHERE LOWER(customer_name) = LOWER($1) OR LOWER(customer_email) = LOWER($2)
       ORDER BY created_at DESC LIMIT 50`,
      [customerName, c.email || '']
    );
    res.json({ success: true, invoices: invoicesResult.rows });
  } catch (error) {
    console.error('Error fetching customer invoices:', error);
    serverError(res, error);
  }
});


router.post('/api/import-customers', upload.single('csvfile'), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ success: false, error: 'No CSV file' });
    const csvContent = req.file.buffer.toString('utf-8');
    const lines = csvContent.split('\n');
    const headers = parseCSVLine(lines[0]);
    
    const customers = [];
    for (let i = 1; i < lines.length; i++) {
      const line = lines[i].trim();
      if (!line) continue;
      try {
        const values = parseCSVLine(line);
        const customer = {};
        headers.forEach((h, idx) => { customer[h] = values[idx] || ''; });
        customers.push(customer);
      } catch (e) {}
    }
    
    let imported = 0, updated = 0, skipped = 0;
    for (const c of customers) {
      try {
        const email = c['Email'] || '';
        const customerNumber = c['Customer Number'] || '';
        let existing = { rows: [] };
        if (email) existing = await pool.query('SELECT id FROM customers WHERE email = $1', [email]);
        else if (customerNumber) existing = await pool.query('SELECT id FROM customers WHERE customer_number = $1', [customerNumber]);
        
        if (existing.rows.length > 0) {
          await pool.query(`UPDATE customers SET name = $1, status = $2, phone = $3, mobile = $4, street = $5, city = $6, state = $7, postal_code = $8, tags = $9, notes = $10, updated_at = CURRENT_TIMESTAMP WHERE id = $11`,
            [c['Name'], c['Status'] || 'Active', c['Phone'], c['Mobile'], c['Street'], c['City'], c['State'], c['Postal Code'], c['Tags'], c['Notes'], existing.rows[0].id]);
          updated++;
        } else {
          await pool.query(`INSERT INTO customers (customer_number, name, status, email, phone, mobile, street, street2, city, state, postal_code, tags, notes) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)`,
            [customerNumber, c['Name'], c['Status'] || 'Active', email, c['Phone'], c['Mobile'], c['Street'], c['Street2'], c['City'], c['State'], c['Postal Code'], c['Tags'], c['Notes']]);
          imported++;
        }
      } catch (e) { skipped++; }
    }
    res.json({ success: true, message: 'Import complete', stats: { total: customers.length, imported, updated, skipped } });
  } catch (error) { serverError(res, error); }
});


router.get('/api/customers/:id/statement-pdf', async (req, res) => {
  try {
    const custResult = await pool.query('SELECT * FROM customers WHERE id = $1', [req.params.id]);
    if (custResult.rows.length === 0) return res.status(404).json({ success: false, error: 'Customer not found' });
    const customer = custResult.rows[0];
    const { from, to, status } = req.query;
    let query = 'SELECT * FROM invoices WHERE customer_id = $1';
    const params = [customer.id];
    let p = 2;
    if (from) { query += ` AND created_at >= $${p++}`; params.push(from); }
    if (to) { query += ` AND created_at <= $${p++}`; params.push(to); }
    if (status) { query += ` AND status = $${p++}`; params.push(status); }
    query += ' ORDER BY created_at DESC';
    const invResult = await pool.query(query, params);
    const dateRange = from || to ? `${from || 'All'} to ${to || 'Present'}` : '';
    const pdfResult = await generateStatementPDF(customer, invResult.rows, dateRange);
    if (!pdfResult || !pdfResult.bytes) return res.status(500).json({ error: 'Statement PDF generation failed' });
    const custName = (customer.name || customer.first_name || 'customer').replace(/\s+/g, '-').toLowerCase();
    res.setHeader('Content-Type', 'application/pdf');
    res.setHeader('Content-Disposition', `inline; filename="statement-${custName}.pdf"`);
    res.send(Buffer.from(pdfResult.bytes));
  } catch (error) {
    console.error('Statement PDF error:', error);
    serverError(res, error);
  }
});

  return router;
};
