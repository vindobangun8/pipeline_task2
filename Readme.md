# Data Pipeline Documentation

## 1. Data Source
**Source Systems**: 

- **Source Type** : PostgresSQL Database
  - Database Name : `source_db`
  - Tables Used :
    - `customers`
    - `employees`
    - `orders`
    - `products`
    - `order_details`
    - `inventory_tracking`
- **Source Type :** Google Sheets
  - Worksheets : `store_branch_paccafe`
  - Sheets : 
    - `store_branch`
  

## 2. Data Sink
  - **Staging :** PostgresSQL
  - **Data Warehouse :** PostgresSQL
  - **Log Message Storage:** Minio, PostgresSQL
  - **Purpose :**
    - Staging : Temporary storage for raw data from source before transformations
    - Warehouse: Cleaned, transformed, and structured data



## 3. Tech Stack

- **ETL** : Pandas, SQLAlchemy 
- **Data Storage :** PostgresSQL (Source , Staging, & Warehouse) , Minio
- **Container :** Docker
- **Version Control :** Git

## 4. Requirement Gathering
| Requirement | Solutions |
|----------|----------|
| Extract Data from source_db | Using full load extractions from source_db |
| Extract Data from Google Sheets | Using full load extractions from Google Sheets |
| Store raw data in staging database | Extract and Load data into staging database PostgreSQL |
| Transform and clean data | Use Pandas (Python) to clean and standardize the data |
| Load cleaned data into warehouse | Perform ETL data from Staging database(PostgreSQL) into Warehouse database(PostgreSQL) |



## 5. Data Pipeline Design

![](C:\Users\Hype\Downloads\Data Pipeline Schema.png)

- **Source Layer**

  - Connect to `sales_db` 

  - Extract all tables (e.g. `customers`,`products`, etc.)

  - Connect to `store_branch_paccofee` Worksheets
  - Extract `store_branch` sheets

- **Staging Layer**
  - Load all data (database & google Sheets) and store them in Staging Database

- **Warehouse Layer**

  - Transform data: normalize, deduplicate, convert data types

  - Load transformed data into warehouse database

## Data Validation

Validation Rule:
1. **Check Missing Value**:
    - Check Missing Value for each column in the table
2. **Date Validation**:
    - Ensure that date columns has valid date format
3. **Numeric Validation:**
    - Ensure that columns contain valid numeric values
    - List of Numeric Columns: 
      - `products`: unit_price, cost_price
      - `orders` : total_amount
      - `order_details`: unit_price, quantity, subtotal
      - `inventory_tracking`: quantity_change
      - `customers`: loyalty_points
4. **Negative Value Validation:**
    - Ensure that columns have a positive number.
    - List of Columns:
      - `products:` unit_price, cost_price
      - `orders`: total_amount
      - `order_details`: unit_price, quantity, subtotal
      - `inventory_tracking`: quantity_change
      - `customers`: loyalty_points

## Source to Target Mapping

Source: Staging
Target: Warehouse

- Source Table: customers

- Target Table: dim_customers

  | Source Field   | Target Field   | Transformation Rule                           |
  | -------------- | -------------- | --------------------------------------------- |
  | -              | sk_customer_id | Auto Generated using `uuid_generate_v4()`     |
  | customer_id    | nk_customer_id | Direct Mapping                                |
  | first_name     | first_name     | Direct Mapping                                |
  | last_name      | last_name      | Direct Mapping                                |
  | email          | email          | Direct Mapping                                |
  | phone          | phone          | Direct Mapping                                |
  | loyalty_points | loyalty_points | Direct Mapping                                |
  | -              | created_at     | Generated in pipeline using current timestamp |

  

Source: Staging
Target: Warehouse

- Source Table: store_branch

- Target Table: dim_store_branch

  | Source Field | Target Field   | Transformation Rule                           |
  | ------------ | -------------- | --------------------------------------------- |
  | -            | sk_employee_id | Auto Generated using `uuid_generate_v4()`     |
  | employee_id  | nk_employee_id | Direct Mapping                                |
  | first_name   | first_name     | Direct Mapping                                |
  | last_name    | last_name      | Direct Mapping                                |
  | hire_date    | hire_date      | Convert Date to INT Format (YYYYMMDD)         |
  | role         | role           | Direct Mapping                                |
  | email        | email          | Direct Mapping                                |
  | -            | created_at     | Generated in pipeline using current timestamp |

  

Source: Staging
Target: Warehouse

- Source Table: employees

- Target Table: dim_employees

  | Source Field | Target Field | Transformation Rule                           |
  | ------------ | ------------ | --------------------------------------------- |
  | -            | sk_store_id  | Auto Generated using `uuid_generate_v4()`     |
  | store_id     | nk_store_id  | Direct Mapping                                |
  | -            | created_at   | Generated in pipeline using current timestamp |



Source: Staging
Target: Warehouse

- Source Table: products

- Target Table: dim_products

  | Source Field | Target Field    | Transformation Rule                                          |
  | ------------ | --------------- | ------------------------------------------------------------ |
  | -            | sk_product_id   | Auto Generated using `uuid_generate_v4()`                    |
  | product_id   | nk_employee_id  | Direct Mapping                                               |
  | store_branch | sk_store_branch | Lookup `sk_store_branch` from `dim_store_branch` table based on `store_branch` (`products`) and `store_name`(`dim_store_branch`) |
  | product_name | product_name    | Direct Mapping                                               |
  | category     | category        | Direct Mapping                                               |
  | unit_price   | unit_price      | Direct Mapping                                               |
  | cost_price   | cost_price      | Direct Mapping                                               |
  | in_stock     | in_stock        | Direct Mapping                                               |
  | -            | created_at      | Generated in pipeline using current timestamp                |

  

Source: Staging
Target: Warehouse

- Source Table: order

- Target Table: fct_order

  | Source Field   | Target Field   | Transformation Rule                                          |
  | -------------- | -------------- | ------------------------------------------------------------ |
  | -              | sk_order_id    | Auto Generated using `uuid_generate_v4()`                    |
  | order_id       | nk_order_id    | Direct Mapping                                               |
  | employee_id    | sk_employee_id | Lookup `sk_employee_id` from `dim_employee` table based on `employee_id` |
  | customer_id    | sk_customer_id | Lookup `sk_customer_id` from `dim_customers` table based on `customer_id` |
  | product_id     | sk_product_id  | Lookup `sk_product_id` from `dim_products` table based on `product_id` |
  | order_date     | order_date     | Convert Date to INT Format (YYYYMMDD)                        |
  | total_amount   | total_amount   | Direct Mapping                                               |
  | quantity       | quantity       | Direct Mapping                                               |
  | unit_price     | unit_price     | Direct Mapping                                               |
  | subtotal       | subtotal       | Direct Mapping                                               |
  | payment_method | payment_method | Direct Mapping                                               |
  | order_status   | order_status   | Direct Mapping                                               |
  | -              | created_at     | Generated in pipeline using current timestamp                |

  

Source: Staging
Target: Warehouse

- Source Table: inventory_tracking

- Target Table: fct_inventory

  | Source Field    | Target Field    | Transformation Rule                                          |
  | --------------- | --------------- | ------------------------------------------------------------ |
  | -               | sk_tracking_id  | Auto Generated using `uuid_generate_v4()`                    |
  | inventory_id    | nk_tracking_id  | Direct Mapping                                               |
  | product_id      | sk_product_id   | Lookup `sk_product_id` from `dim_products` table based on `product_id` |
  | quantity_change | quantity_change | Direct mapping                                               |
  | change_date     | change_date     | Convert Date to INT Format (YYYYMMDD)                        |
  | reason          | reason          | Direct Mapping                                               |
  | -               | created_at      | Generated in pipeline using current timestamp                |

