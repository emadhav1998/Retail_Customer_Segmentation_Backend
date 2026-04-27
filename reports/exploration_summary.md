# Data Exploration Summary Report

## Overview

This report summarizes the data exploration and quality analysis results for the retail customer segmentation dataset.

---

## 1. Null Analysis

### Summary
| Column | Null Count | Null Percentage | Non-Null Count |
|--------|------------|-----------------|----------------|
| TBD | TBD | TBD | TBD |

### Key Findings
- **Critical Columns**: CustomerID, InvoiceDate, Quantity, UnitPrice
- **Action Required**: 
  - Remove rows with null CustomerID (cannot segment)
  - Handle null InvoiceDate values
  - Validate null Quantity/UnitPrice

---

## 2. Duplicate Analysis

### Summary
| Metric | Value |
|--------|-------|
| Total Rows | TBD |
| Duplicate Rows | TBD |
| Duplicate Percentage | TBD% |
| Key Column Duplicates | TBD |

### Key Findings
- **Exact Duplicates**: Full row duplicates should be removed
- **Invoice Duplicates**: Multiple items in same invoice
- **Action Required**: Remove duplicate rows before processing

---

## 3. Invalid Value Analysis

### Quantity Analysis
- **Negative Quantities**: May indicate returns
- **Zero Quantities**: Invalid transactions

### UnitPrice Analysis
- **Negative Prices**: Invalid, need correction
- **Zero Prices**: May be free items or errors

### CustomerID Analysis
- **Null CustomerID**: Cannot perform customer segmentation
- **Action Required**: Filter out or impute missing CustomerID

### InvoiceDate Analysis
- **Invalid Dates**: Cannot calculate Recency
- **Future Dates**: Data entry errors
- **Action Required**: Validate and clean date values

---

## 4. Column Profile Summary

### Numeric Columns
| Column | Mean | Median | Min | Max | Std Dev |
|--------|------|--------|-----|-----|---------|
| Quantity | TBD | TBD | TBD | TBD | TBD |
| UnitPrice | TBD | TBD | TBD | TBD | TBD |

### Categorical Columns
| Column | Unique Values | Most Common |
|--------|---------------|-------------|
| Country | TBD | TBD |
| StockCode | TBD | TBD |

### Date Columns
| Column | Min Date | Max Date | Range |
|--------|----------|----------|-------|
| InvoiceDate | TBD | TBD | TBD days |

---

## 5. Recommendations

### Data Cleaning Steps
1. **Remove Duplicates**: Drop exact duplicate rows
2. **Handle Missing CustomerID**: Remove or flag rows
3. **Fix Invalid Values**: Correct negative quantities/prices
4. **Standardize Dates**: Parse and validate InvoiceDate
5. **Remove Cancelled Orders**: Filter out invoices starting with 'C'

### Next Steps
- Proceed to data preprocessing
- Build RFM metrics
- Perform customer clustering

---

## 6. Data Quality Score

| Metric | Score |
|--------|-------|
| Completeness | TBD% |
| Validity | TBD% |
| Consistency | TBD% |
| Overall | TBD% |

---

*Generated on: [Date]*
*Dataset ID: [ID]*