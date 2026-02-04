# Data Feed Intake Form

A Databricks App for capturing metadata about outbound data feeds.

## Files

```
databricks_app/
├── app.py              # Streamlit application
├── app.yaml            # Databricks App config
├── create_table.sql    # Delta table DDL
├── requirements.txt    # Python dependencies
└── README.md
```

## Setup

### 1. Create the Delta table

Run `create_table.sql` in Databricks SQL or a notebook:

```sql
%run ./create_table.sql
```

Or copy/paste the contents into a SQL editor.

### 2. Deploy the App

**Option A: Databricks CLI**
```bash
cd databricks_app/
databricks apps deploy --app-name data-feed-intake-form
```

**Option B: UI**
1. Go to Workspace → Apps → Create App
2. Upload all files
3. Configure and deploy

### 3. Access the App

Once deployed, access via the URL provided by Databricks Apps.

## Usage

1. Enter your email in the sidebar to see your previous requests
2. Fill in the form fields
3. Click "Save Draft" to save progress, or "Submit" when ready
4. After submission, mark as "Complete" when requirements are finalized

## Status Workflow

- **draft** → Work in progress, can be edited
- **submitted** → Requirements captured, ready for SQL development
- **complete** → Locked, requirements finalized

## Next Steps (Future Phases)

- Auto-create Git repo when status = complete
- CI/CD integration for pipeline generation
- Unit test validation before deployment
