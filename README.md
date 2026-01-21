# Data Engineering Project Portfolio

End-to-end pipelines for analytics use cases. Focus on healthcare data integration and transformation.

## Projects

### Healthcare Data Integration Pipeline: Multi-Format ETL for Clinical Analytics
**Challenge**: Healthcare data arrives in fragmented formats—FHIR JSON from EHR APIs, HL7 v2 messages from clinical systems, scanned PDFs requiring OCR, and CSV exports. Analysts need unified, query-optimized data for insights on patient outcomes and operational efficiency.

**What I Built**: End-to-end ETL pipeline ingesting four healthcare data formats, normalizing through Spark transformations, applying dbt business logic, and outputting to Parquet for analytics. Handles missing data, format inconsistencies, and cross-format patient matching.

**Technical Approach**:
- Synthetic healthcare data: patient demographics, clinical observations, prescriptions, insurance claims
- Format-specific handlers: FHIR JSON parser, HL7 v2 message parser, PDF OCR extraction, CSV readers
- Spark transformation layer: data quality checks, normalization, patient record enrichment
- dbt models: healthcare business rules (episode grouping, readmissions, costs) and dimensional models
- Parquet storage partitioned by date and cohort for query performance

**Outcome**:  Production-ready pipeline demonstrating heterogeneous data integration, domain-specific transformations, data quality enforcement, and analytical optimization. Includes documentation, automated tests, and reproducible setup.

---

### [Project 2 - Coming Soon]

### [Project 3 - Coming Soon]

## Contact

Questions about these projects? Reach out at sidskonda@gmail.com

---

*All projects use synthetic data. No classified or proprietary information is included.*
