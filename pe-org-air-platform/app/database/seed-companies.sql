CALL cs4_insert_company('Caterpillar Inc.', 'CAT',  '550e8400-e29b-41d4-a716-446655440003', 'manufacturing', 'Heavy Machinery', 0.65, 59427, 109600, 'December');
CALL cs4_insert_company('Deere & Company', 'DE',   '550e8400-e29b-41d4-a716-446655440003', 'manufacturing', 'Agricultural Equipment', 0.70, 52577, 82000, 'October');

CALL cs4_insert_company('UnitedHealth Group', 'UNH','550e8400-e29b-41d4-a716-446655440002', 'healthcare', 'Managed Care', 0.90, 371622, 440000, 'December');
CALL cs4_insert_company('HCA Healthcare', 'HCA',   '550e8400-e29b-41d4-a716-446655440002', 'healthcare', 'Hospital Systems', 0.75, 64968, 309000, 'December');

CALL cs4_insert_company('Automatic Data Processing', 'ADP','550e8400-e29b-41d4-a716-446655440003', 'technology', 'HR Technology', 0.80, 18672, 58000, 'June');
CALL cs4_insert_company('Paychex Inc.', 'PAYX',      '550e8400-e29b-41d4-a716-446655440003', 'technology', 'Payroll Services', 0.60, 5278, 16000, 'May');

CALL cs4_insert_company('Walmart Inc.', 'WMT', '550e8400-e29b-41d4-a716-446655440004', 'retail', 'Discount Retail', 0.60, 648125, 2100000, 'January');
CALL cs4_insert_company('Target Corporation', 'TGT','550e8400-e29b-41d4-a716-446655440004', 'retail', 'Discount Retail', 0.55, 109120, 440000, 'January');

CALL cs4_insert_company('JPMorgan Chase', 'JPM','550e8400-e29b-41d4-a716-446655440005', 'financial_services', 'Investment Banking', 0.85, 158104, 308669, 'December');
CALL cs4_insert_company('Goldman Sachs', 'GS', '550e8400-e29b-41d4-a716-446655440005', 'financial_services', 'Investment Banking', 0.80, 46254, 44600, 'December');

-- CS4 Portfolio seed
INSERT INTO cs4_portfolios (id, name, fund_vintage)
VALUES ('a1b2c3d4-e5f6-7890-abcd-ef1234567890', 'PE OrgAIR Fund I', 2022);

INSERT INTO cs4_portfolio_companies (portfolio_id, company_id)
SELECT 'a1b2c3d4-e5f6-7890-abcd-ef1234567890', id
FROM companies
WHERE ticker IN ('CAT', 'DE', 'UNH', 'HCA', 'ADP', 'PAYX', 'WMT', 'TGT', 'JPM', 'GS')
  AND is_deleted = FALSE;
