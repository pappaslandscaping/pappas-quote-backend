function buildDashboardActivityFeedQuery() {
  return `
      (
        SELECT 'quote_sent' as type,
               'Quote sent to ' || COALESCE(customer_name, 'Unknown') || ' — ' || COALESCE(quote_number, 'Q-' || id::text) as description,
               created_at as timestamp,
               '/sent-quote-detail.html?id=' || id as link
        FROM sent_quotes
        WHERE status IN ('sent','viewed')
        ORDER BY created_at DESC LIMIT 10
      )
      UNION ALL
      (
        SELECT 'quote_signed' as type,
               COALESCE(customer_name, 'Unknown') || ' signed quote ' || COALESCE(quote_number, 'Q-' || id::text) as description,
               COALESCE(signed_at, updated_at, created_at) as timestamp,
               '/sent-quote-detail.html?id=' || id as link
        FROM sent_quotes
        WHERE status IN ('signed','contracted')
        ORDER BY COALESCE(signed_at, updated_at, created_at) DESC LIMIT 10
      )
      UNION ALL
      (
        SELECT 'invoice_created' as type,
               'Invoice ' || COALESCE(invoice_number, '#' || id::text) || ' created for ' || COALESCE(customer_name, 'Unknown') || ' — $' || COALESCE(total::text, '0') as description,
               created_at as timestamp,
               '/invoice-detail.html?id=' || id as link
        FROM invoices
        WHERE status = 'draft' OR status = 'sent'
        ORDER BY created_at DESC LIMIT 10
      )
      UNION ALL
      (
        SELECT 'payment_received' as type,
               'Payment received from ' || COALESCE(customer_name, 'Unknown') || ' — $' || COALESCE(amount_paid::text, '0') as description,
               paid_at as timestamp,
               '/invoice-detail.html?id=' || id as link
        FROM invoices
        WHERE status = 'paid' AND amount_paid > 0 AND paid_at IS NOT NULL
        ORDER BY paid_at DESC LIMIT 10
      )
      UNION ALL
      (
        SELECT 'job_completed' as type,
               'Job completed: ' || COALESCE(service_type, 'Service') || ' for ' || COALESCE(customer_name, 'Unknown') as description,
               COALESCE(updated_at, job_date::timestamp) as timestamp,
               '/job-detail.html?id=' || id as link
        FROM scheduled_jobs
        WHERE status IN ('completed','done')
        ORDER BY COALESCE(updated_at, job_date::timestamp) DESC LIMIT 10
      )
      UNION ALL
      (
        SELECT 'job_scheduled' as type,
               'Job scheduled: ' || COALESCE(service_type, 'Service') || ' for ' || COALESCE(customer_name, 'Unknown') || ' on ' || to_char(job_date, 'Mon DD') as description,
               created_at as timestamp,
               '/job-detail.html?id=' || id as link
        FROM scheduled_jobs
        WHERE status IN ('pending','scheduled') AND job_date >= CURRENT_DATE
        ORDER BY created_at DESC LIMIT 10
      )
      UNION ALL
      (
        SELECT 'new_customer' as type,
               'New customer: ' || COALESCE(name, 'Unknown') as description,
               created_at as timestamp,
               '/customer-detail.html?id=' || id as link
        FROM customers
        ORDER BY created_at DESC LIMIT 10
      )
      ORDER BY timestamp DESC NULLS LAST
      LIMIT 20
    `;
}

module.exports = { buildDashboardActivityFeedQuery };
