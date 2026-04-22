# Merge Tags

This document defines the approved variable mapping for email templates across the backend and Copilot CRM.

## Rule

Use a canonical variable name for planning and documentation, then render the correct token per platform.

- Backend templates use tokens like `{customer_first_name}`
- Copilot templates use tokens like `{{CUSTOMER_FIRST_NAME}}`

Do not mix formats inside the same template.

## Core Contact Fields

| Canonical Key | Backend Token | Copilot Token |
|---|---|---|
| customer_first_name | `{customer_first_name}` | `{{CUSTOMER_FIRST_NAME}}` |
| customer_last_name | `{customer_last_name}` | `{{CUSTOMER_LAST_NAME}}` |
| customer_name | `{customer_name}` | `{{CUSTOMER_NAME}}` |
| customer_company_name | `{customer_company_name}` | `{{CUSTOMER_COMPANY_NAME}}` |
| customer_phone | `{customer_phone}` | `{{CUSTOMER_PHONE}}` |
| customer_cell | `{customer_cell}` | `{{CUSTOMER_CELL}}` |
| customer_account_balance | `{customer_account_balance}` | `{{CUSTOMER_ACCOUNT_BALANCE}}` |
| customer_address | `{customer_address}` | `{{CUSTOMER_ADDRESS}}` |
| customer_city | `{customer_city}` | `{{CUSTOMER_CITY}}` |
| customer_state | `{customer_state}` | `{{CUSTOMER_STATE}}` |
| customer_zip | `{customer_zip}` | `{{CUSTOMER_ZIP}}` |

## Company Fields

| Canonical Key | Backend Token | Copilot Token |
|---|---|---|
| company_owner_first_name | `{company_owner_first_name}` | `{{COMPANY_OWNER_FIRST_NAME}}` |
| company_owner_last_name | `{company_owner_last_name}` | `{{COMPANY_OWNER_LAST_NAME}}` |
| company_name | `{company_name}` | `{{COMPANY_NAME}}` |
| company_phone | `{company_phone}` | `{{COMPANY_PHONE}}` |
| company_website | `{company_website}` | `{{COMPANY_WEBSITE}}` |
| company_email | `{company_email}` | `{{COMPANY_EMAIL}}` |

## Employee Fields

| Canonical Key | Backend Token | Copilot Token |
|---|---|---|
| employee_first_name | `{employee_first_name}` | `{{EMPLOYEE_FIRST_NAME}}` |
| employee_last_name | `{employee_last_name}` | `{{EMPLOYEE_LAST_NAME}}` |

## Estimate Fields

| Canonical Key | Backend Token | Copilot Token |
|---|---|---|
| estimate_number | `{estimate_number}` | `{{ESTIMATE_NUMBER}}` |
| estimate_total | `{estimate_total}` | `{{ESTIMATE_TOTAL}}` |
| estimate_link | `{estimate_link}` | `{{ESTIMATE_LINK}}` |
| estimate_edit_link | `{estimate_edit_link}` | `{{ESTIMATE_EDIT_LINK}}` |
| estimate_changes | `{estimate_changes}` | `{{ESTIMATE_CHANGES}}` |

## Invoice Fields

| Canonical Key | Backend Token | Copilot Token |
|---|---|---|
| invoice_number | `{invoice_number}` | `{{INVOICE_NUMBER}}` |
| invoice_tax | `{invoice_tax}` | `{{INVOICE_TAX}}` |
| invoice_total | `{invoice_total}` | `{{INVOICE_TOTAL}}` |
| invoice_due_date | `{invoice_due_date}` | `{{INVOICE_DUE_DATE}}` |
| invoices_links | `{invoices_links}` | `{{INVOICES_LINKS}}` |

## Payment Fields

| Canonical Key | Backend Token | Copilot Token |
|---|---|---|
| payment_amount | `{payment_amount}` | `{{PAYMENT_AMOUNT}}` |
| payment_method | `{payment_method}` | `{{PAYMENT_METHOD}}` |
| payment_date | `{payment_date}` | `{{PAYMENT_DATE}}` |
| receipt_link | `{receipt_link}` | `{{RECEIPT_LINK}}` |
| receipt_admin_link | `{receipt_admin_link}` | `{{RECEIPT_ADMIN_LINK}}` |

## Portal And Links

| Canonical Key | Backend Token | Copilot Token |
|---|---|---|
| customer_portal_link | `{customer_portal_link}` | `{{CUSTOMER_PORTAL_LINK}}` |
| customer_portal_add_credit_card_link | `{customer_portal_add_credit_card_link}` | `{{CUSTOMER_PORTAL_ADD_CREDIT_CARD_LINK}}` |
| unsubscribe | `{unsubscribe}` | `{{UNSUBSCRIBE}}` |
| work_request_form | `{work_request_form}` | `{{WORK_REQUEST_FORM}}` |
| docs_links | `{docs_links}` | `{{DOCS_LINKS}}` |
| example_up_sell_link | `{example_up_sell_link}` | `{{EXAMPLE_UP_SELL_LINK}}` |

## Event And Service Fields

| Canonical Key | Backend Token | Copilot Token |
|---|---|---|
| call_notes | `{call_notes}` | `{{CALL_NOTES}}` |
| event_title | `{event_title}` | `{{EVENT_TITLE}}` |
| event_date | `{event_date}` | `{{EVENT_DATE}}` |
| event_property_address | `{event_property_address}` | `{{EVENT_PROPERTY_ADDRESS}}` |
| event_scheduled_time | `{event_scheduled_time}` | `{{EVENT_SCHEDULED_TIME}}` |
| event_notes | `{event_notes}` | `{{EVENT_NOTES}}` |
| property_not_serviced_reason | `{property_not_serviced_reason}` | `{{PROPERTY_NOT_SERVICED_REASON}}` |
| notes_to_dispatcher | `{notes_to_dispatcher}` | `{{NOTES_TO_DISPATCHER}}` |

## Date Fields

| Canonical Key | Backend Token | Copilot Token |
|---|---|---|
| today | `{today}` | `{{TODAY}}` |
| current_month | `{current_month}` | `{{CURRENT_MONTH}}` |
| current_year | `{current_year}` | `{{CURRENT_YEAR}}` |

## Notes

- Copilot tokens should be used exactly as provided, including uppercase letters and double curly braces.
- Backend tokens should stay lowercase with single curly braces unless the backend renderer is explicitly changed.
- Do not build one template that mixes backend and Copilot token styles.
- If a new token is introduced in either system, add it here before using it in production templates.
