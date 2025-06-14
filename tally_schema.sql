-- Configuration table
CREATE TABLE config (
  name varchar(64) NOT NULL PRIMARY KEY,
  value varchar(1024),
  user_id varchar(255),
  company_name varchar(255)
);

-- Master Tables
CREATE TABLE mst_group (
  guid varchar(64) NOT NULL PRIMARY KEY,
  name varchar(1024) NOT NULL DEFAULT '',
  parent varchar(1024) NOT NULL DEFAULT '',
  primary_group varchar(1024) NOT NULL DEFAULT '',
  is_revenue smallint,
  is_deemedpositive smallint,
  is_reserved smallint,
  affects_gross_profit smallint,
  sort_position integer,
  user_id varchar(255),
  company_name varchar(255)
);

CREATE TABLE mst_ledger (
  guid varchar(64) NOT NULL PRIMARY KEY,
  name varchar(1024) NOT NULL DEFAULT '',
  parent varchar(1024) NOT NULL DEFAULT '',
  alias varchar(256) NOT NULL DEFAULT '',
  description varchar(64) NOT NULL DEFAULT '',
  notes varchar(64) NOT NULL DEFAULT '',
  is_revenue smallint,
  is_deemedpositive smallint,
  opening_balance numeric DEFAULT 0,
  closing_balance numeric DEFAULT 0,
  mailing_name varchar(256) NOT NULL DEFAULT '',
  mailing_address varchar(1024) NOT NULL DEFAULT '',
  mailing_state varchar(256) NOT NULL DEFAULT '',
  mailing_country varchar(256) NOT NULL DEFAULT '',
  mailing_pincode varchar(64) NOT NULL DEFAULT '',
  email varchar(256) NOT NULL DEFAULT '',
  it_pan varchar(64) NOT NULL DEFAULT '',
  gstn varchar(64) NOT NULL DEFAULT '',
  gst_registration_type varchar(64) NOT NULL DEFAULT '',
  gst_supply_type varchar(64) NOT NULL DEFAULT '',
  gst_duty_head varchar(16) NOT NULL DEFAULT '',
  tax_rate numeric DEFAULT 0,
  bank_account_holder varchar(256) NOT NULL DEFAULT '',
  bank_account_number varchar(64) NOT NULL DEFAULT '',
  bank_ifsc varchar(64) NOT NULL DEFAULT '',
  bank_swift varchar(64) NOT NULL DEFAULT '',
  bank_name varchar(64) NOT NULL DEFAULT '',
  bank_branch varchar(64) NOT NULL DEFAULT '',
  bill_credit_period integer NOT NULL DEFAULT 0,
  user_id varchar(255),
  company_name varchar(255)
);

CREATE TABLE mst_vouchertype (
  guid varchar(64) NOT NULL PRIMARY KEY,
  name varchar(1024) NOT NULL DEFAULT '',
  parent varchar(1024) NOT NULL DEFAULT '',
  numbering_method varchar(64) NOT NULL DEFAULT '',
  is_deemedpositive smallint,
  affects_stock smallint,
  user_id varchar(255),
  company_name varchar(255)
);

CREATE TABLE mst_uom (
  guid varchar(64) NOT NULL PRIMARY KEY,
  name varchar(1024) NOT NULL DEFAULT '',
  formalname varchar(256) NOT NULL DEFAULT '',
  is_simple_unit smallint NOT NULL,
  base_units varchar(1024) NOT NULL,
  additional_units varchar(1024) NOT NULL,
  conversion integer NOT NULL,
  user_id varchar(255),
  company_name varchar(255)
);

CREATE TABLE mst_godown (
  guid varchar(64) NOT NULL PRIMARY KEY,
  name varchar(1024) NOT NULL DEFAULT '',
  parent varchar(1024) NOT NULL DEFAULT '',
  address varchar(1024) NOT NULL DEFAULT '',
  user_id varchar(255),
  company_name varchar(255)
);

CREATE TABLE mst_stock_group (
  guid varchar(64) NOT NULL PRIMARY KEY,
  name varchar(1024) NOT NULL DEFAULT '',
  parent varchar(1024) NOT NULL DEFAULT '',
  user_id varchar(255),
  company_name varchar(255)
);

CREATE TABLE mst_stock_item (
  guid varchar(64) NOT NULL PRIMARY KEY,
  name varchar(1024) NOT NULL DEFAULT '',
  parent varchar(1024) NOT NULL DEFAULT '',
  alias varchar(256) NOT NULL DEFAULT '',
  description varchar(64) NOT NULL DEFAULT '',
  notes varchar(64) NOT NULL DEFAULT '',
  part_number varchar(256) NOT NULL DEFAULT '',
  uom varchar(32) NOT NULL DEFAULT '',
  alternate_uom varchar(32) NOT NULL DEFAULT '',
  conversion integer NOT NULL DEFAULT 0,
  opening_balance numeric DEFAULT 0,
  opening_rate numeric DEFAULT 0,
  opening_value numeric DEFAULT 0,
  closing_balance numeric DEFAULT 0,
  closing_rate numeric DEFAULT 0,
  closing_value numeric DEFAULT 0,
  costing_method varchar(32) NOT NULL DEFAULT '',
  gst_type_of_supply varchar(32) DEFAULT '',
  gst_hsn_code varchar(64) DEFAULT '',
  gst_hsn_description varchar(256) DEFAULT '',
  gst_rate numeric DEFAULT 0,
  gst_taxability varchar(32) DEFAULT '',
  user_id varchar(255),
  company_name varchar(255)
);

CREATE TABLE mst_cost_category (
  guid varchar(64) NOT NULL PRIMARY KEY,
  name varchar(1024) NOT NULL DEFAULT '',
  allocate_revenue smallint,
  allocate_non_revenue smallint,
  user_id varchar(255),
  company_name varchar(255)
);

CREATE TABLE mst_cost_centre (
  guid varchar(64) NOT NULL PRIMARY KEY,
  name varchar(1024) NOT NULL DEFAULT '',
  parent varchar(1024) NOT NULL DEFAULT '',
  category varchar(1024) NOT NULL DEFAULT '',
  user_id varchar(255),
  company_name varchar(255)
);

CREATE TABLE mst_attendance_type (
  guid varchar(64) NOT NULL PRIMARY KEY,
  name varchar(1024) NOT NULL DEFAULT '',
  parent varchar(1024) NOT NULL DEFAULT '',
  uom varchar(32) NOT NULL DEFAULT '',
  attendance_type varchar(64) NOT NULL DEFAULT '',
  attendance_period varchar(64) NOT NULL DEFAULT '',
  user_id varchar(255),
  company_name varchar(255)
);

CREATE TABLE mst_employee (
  guid varchar(64) NOT NULL PRIMARY KEY,
  name varchar(1024) NOT NULL DEFAULT '',
  parent varchar(1024) NOT NULL DEFAULT '',
  id_number varchar(256) NOT NULL DEFAULT '',
  date_of_joining date,
  date_of_release date,
  designation varchar(64) NOT NULL DEFAULT '',
  function_role varchar(64) NOT NULL DEFAULT '',
  location varchar(256) NOT NULL DEFAULT '',
  gender varchar(32) NOT NULL DEFAULT '',
  date_of_birth date,
  blood_group varchar(32) NOT NULL DEFAULT '',
  father_mother_name varchar(256) NOT NULL DEFAULT '',
  spouse_name varchar(256) NOT NULL DEFAULT '',
  address varchar(256) NOT NULL DEFAULT '',
  mobile varchar(32) NOT NULL DEFAULT '',
  email varchar(64) NOT NULL DEFAULT '',
  pan varchar(32) NOT NULL DEFAULT '',
  aadhar varchar(32) NOT NULL DEFAULT '',
  uan varchar(32) NOT NULL DEFAULT '',
  pf_number varchar(32) NOT NULL DEFAULT '',
  pf_joining_date date,
  pf_relieving_date date,
  pr_account_number varchar(32) NOT NULL DEFAULT '',
  user_id varchar(255),
  company_name varchar(255)
);

CREATE TABLE mst_payhead (
  guid varchar(64) NOT NULL PRIMARY KEY,
  name varchar(1024) NOT NULL DEFAULT '',
  parent varchar(1024) NOT NULL DEFAULT '',
  payslip_name varchar(1024) NOT NULL DEFAULT '',
  pay_type varchar(64) NOT NULL DEFAULT '',
  income_type varchar(64) NOT NULL DEFAULT '',
  calculation_type varchar(32) NOT NULL DEFAULT '',
  leave_type varchar(64) NOT NULL DEFAULT '',
  calculation_period varchar(32) NOT NULL DEFAULT '',
  user_id varchar(255),
  company_name varchar(255)
);

CREATE TABLE mst_gst_effective_rate (
  item varchar(1024) NOT NULL DEFAULT '',
  applicable_from date,
  hsn_description varchar(256) NOT NULL DEFAULT '',
  hsn_code varchar(64) NOT NULL DEFAULT '',
  rate numeric DEFAULT 0,
  is_rcm_applicable smallint,
  nature_of_transaction varchar(64) NOT NULL DEFAULT '',
  nature_of_goods varchar(64) NOT NULL DEFAULT '',
  supply_type varchar(64) NOT NULL DEFAULT '',
  taxability varchar(64) NOT NULL DEFAULT '',
  user_id varchar(255),
  company_name varchar(255)
);

CREATE TABLE mst_opening_batch_allocation (
  name varchar(1024) NOT NULL DEFAULT '',
  item varchar(1024) NOT NULL DEFAULT '',
  opening_balance numeric DEFAULT 0,
  opening_rate numeric DEFAULT 0,
  opening_value numeric DEFAULT 0,
  godown varchar(1024) NOT NULL DEFAULT '',
  manufactured_on date,
  user_id varchar(255),
  company_name varchar(255)
);

CREATE TABLE mst_opening_bill_allocation (
  ledger varchar(1024) NOT NULL DEFAULT '',
  opening_balance numeric DEFAULT 0,
  bill_date date,
  name varchar(1024) NOT NULL DEFAULT '',
  bill_credit_period integer NOT NULL DEFAULT 0,
  is_advance smallint,
  user_id varchar(255),
  company_name varchar(255)
);

CREATE TABLE trn_closingstock_ledger (
  ledger varchar(1024) NOT NULL DEFAULT '',
  stock_date date,
  stock_value numeric NOT NULL DEFAULT 0,
  user_id varchar(255),
  company_name varchar(255)
);

CREATE TABLE mst_stockitem_standard_cost (
  item varchar(1024) NOT NULL DEFAULT '',
  date date,
  rate numeric DEFAULT 0,
  user_id varchar(255),
  company_name varchar(255)
);

CREATE TABLE mst_stockitem_standard_price (
  item varchar(1024) NOT NULL DEFAULT '',
  date date,
  rate numeric DEFAULT 0,
  user_id varchar(255),
  company_name varchar(255)
);

-- Transaction Tables
CREATE TABLE trn_voucher (
  guid varchar(64) NOT NULL PRIMARY KEY,
  date date NOT NULL,
  voucher_type varchar(1024) NOT NULL,
  voucher_number varchar(64) NOT NULL DEFAULT '',
  reference_number varchar(64) NOT NULL DEFAULT '',
  reference_date date,
  narration varchar(4000) NOT NULL DEFAULT '',
  party_name varchar(256) NOT NULL,
  place_of_supply varchar(256) NOT NULL,
  is_invoice smallint,
  is_accounting_voucher smallint,
  is_inventory_voucher smallint,
  is_order_voucher smallint,
  user_id varchar(255),
  company_name varchar(255)
);

CREATE TABLE trn_accounting (
  guid varchar(64) NOT NULL DEFAULT '',
  ledger varchar(1024) NOT NULL DEFAULT '',
  amount numeric NOT NULL DEFAULT 0,
  amount_forex numeric NOT NULL DEFAULT 0,
  currency varchar(16) NOT NULL DEFAULT '',
  user_id varchar(255),
  company_name varchar(255)
);

CREATE TABLE trn_inventory (
  guid varchar(64) NOT NULL DEFAULT '',
  item varchar(1024) NOT NULL DEFAULT '',
  quantity numeric NOT NULL DEFAULT 0,
  rate numeric NOT NULL DEFAULT 0,
  amount numeric NOT NULL DEFAULT 0,
  additional_amount numeric NOT NULL DEFAULT 0,
  discount_amount numeric NOT NULL DEFAULT 0,
  godown varchar(1024),
  tracking_number varchar(256),
  order_number varchar(256),
  order_duedate date,
  user_id varchar(255),
  company_name varchar(255)
);

CREATE TABLE trn_cost_centre (
  guid varchar(64) NOT NULL DEFAULT '',
  ledger varchar(1024) NOT NULL DEFAULT '',
  costcentre varchar(1024) NOT NULL DEFAULT '',
  amount numeric NOT NULL DEFAULT 0,
  user_id varchar(255),
  company_name varchar(255)
);

CREATE TABLE trn_cost_category_centre (
  guid varchar(64) NOT NULL DEFAULT '',
  ledger varchar(1024) NOT NULL DEFAULT '',
  costcategory varchar(1024) NOT NULL DEFAULT '',
  costcentre varchar(1024) NOT NULL DEFAULT '',
  amount numeric NOT NULL DEFAULT 0,
  user_id varchar(255),
  company_name varchar(255)
);

CREATE TABLE trn_cost_inventory_category_centre (
  guid varchar(64) NOT NULL DEFAULT '',
  ledger varchar(1024) NOT NULL DEFAULT '',
  item varchar(1024) NOT NULL DEFAULT '',
  costcategory varchar(1024) NOT NULL DEFAULT '',
  costcentre varchar(1024) NOT NULL DEFAULT '',
  amount numeric NOT NULL DEFAULT 0,
  user_id varchar(255),
  company_name varchar(255)
);

CREATE TABLE trn_bill (
  guid varchar(64) NOT NULL DEFAULT '',
  ledger varchar(1024) NOT NULL DEFAULT '',
  name varchar(1024) NOT NULL DEFAULT '',
  amount numeric NOT NULL DEFAULT 0,
  billtype varchar(256) NOT NULL DEFAULT '',
  bill_credit_period integer NOT NULL DEFAULT 0,
  user_id varchar(255),
  company_name varchar(255)
);

CREATE TABLE trn_bank (
  guid varchar(64) NOT NULL DEFAULT '',
  ledger varchar(1024) NOT NULL DEFAULT '',
  transaction_type varchar(32) NOT NULL DEFAULT '',
  instrument_date date,
  instrument_number varchar(1024) NOT NULL DEFAULT '',
  bank_name varchar(64) NOT NULL DEFAULT '',
  amount numeric NOT NULL DEFAULT 0,
  bankers_date date,
  user_id varchar(255),
  company_name varchar(255)
);

CREATE TABLE trn_batch (
  guid varchar(64) NOT NULL DEFAULT '',
  item varchar(1024) NOT NULL DEFAULT '',
  name varchar(1024) NOT NULL DEFAULT '',
  quantity numeric NOT NULL DEFAULT 0,
  amount numeric NOT NULL DEFAULT 0,
  godown varchar(1024),
  destination_godown varchar(1024),
  tracking_number varchar(1024),
  user_id varchar(255),
  company_name varchar(255)
);

CREATE TABLE trn_inventory_accounting (
  guid varchar(64) NOT NULL DEFAULT '',
  ledger varchar(1024) NOT NULL DEFAULT '',
  amount numeric NOT NULL DEFAULT 0,
  additional_allocation_type varchar(32) NOT NULL DEFAULT '',
  user_id varchar(255),
  company_name varchar(255)
);

CREATE TABLE trn_employee (
  guid varchar(64) NOT NULL DEFAULT '',
  category varchar(1024) NOT NULL DEFAULT '',
  employee_name varchar(1024) NOT NULL DEFAULT '',
  amount numeric NOT NULL DEFAULT 0,
  employee_sort_order integer NOT NULL DEFAULT 0,
  user_id varchar(255),
  company_name varchar(255)
);

CREATE TABLE trn_payhead (
  guid varchar(64) NOT NULL DEFAULT '',
  category varchar(1024) NOT NULL DEFAULT '',
  employee_name varchar(1024) NOT NULL DEFAULT '',
  employee_sort_order integer NOT NULL DEFAULT 0,
  payhead_name varchar(1024) NOT NULL DEFAULT '',
  payhead_sort_order integer NOT NULL DEFAULT 0,
  amount numeric NOT NULL DEFAULT 0,
  user_id varchar(255),
  company_name varchar(255)
);

CREATE TABLE trn_attendance (
  guid varchar(64) NOT NULL DEFAULT '',
  employee_name varchar(1024) NOT NULL DEFAULT '',
  attendancetype_name varchar(1024) NOT NULL DEFAULT '',
  time_value numeric NOT NULL DEFAULT 0,
  type_value numeric NOT NULL DEFAULT 0,
  user_id varchar(255),
  company_name varchar(255)
);
