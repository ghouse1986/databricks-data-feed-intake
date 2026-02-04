"""
Data Feed Intake Form - Databricks App
Captures metadata for outbound data feeds
Persists to Delta table: ohiadev.data_feeds.intake_requests
"""

import streamlit as st
from datetime import datetime, time
import uuid

from databricks import sql
from databricks.sdk.core import Config

# Databricks configuration
cfg = Config()
SQL_WAREHOUSE_HTTP_PATH = "/sql/1.0/warehouses/2b2165b53c9575f7"

# Constants
CATALOG = "ohiadev"
SCHEMA = "data_feeds"
TABLE_NAME = "intake_requests"
FULL_TABLE_NAME = f"{CATALOG}.{SCHEMA}.{TABLE_NAME}"

# Page configuration
st.set_page_config(
    page_title="Data Feed Intake Form",
    page_icon="📊",
    layout="wide"
)

# Custom CSS
st.markdown("""
<style>
    .main .block-container {
        padding-top: 2rem;
        max-width: 900px;
    }
    h1 {
        color: #1f77b4;
    }
    .stAlert {
        margin-top: 1rem;
    }
</style>
""", unsafe_allow_html=True)


@st.cache_resource
def get_connection():
    """Get cached SQL connection"""
    return sql.connect(
        server_hostname=cfg.host,
        http_path=SQL_WAREHOUSE_HTTP_PATH,
        credentials_provider=lambda: cfg.authenticate,
    )


def generate_request_id():
    """Generate unique request ID"""
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    short_uuid = str(uuid.uuid4())[:8]
    return f"REQ_{timestamp}_{short_uuid}"


def get_existing_requests(requestor_email: str = None):
    """Fetch existing requests from Delta table"""
    try:
        conn = get_connection()
        with conn.cursor() as cursor:
            if requestor_email:
                query = f"SELECT * FROM {FULL_TABLE_NAME} WHERE requestor_email = '{requestor_email}' ORDER BY created_at DESC"
            else:
                query = f"SELECT * FROM {FULL_TABLE_NAME} ORDER BY created_at DESC"
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            return [dict(zip(columns, row)) for row in rows]
    except Exception as e:
        st.warning(f"Could not fetch existing requests: {e}")
        return []


def save_request(data: dict, is_update: bool = False):
    """Save or update request in Delta table"""
    try:
        conn = get_connection()
        with conn.cursor() as cursor:
            if is_update:
                # Build UPDATE statement
                set_clauses = []
                for key, value in data.items():
                    if key != 'request_id':
                        if value is None:
                            set_clauses.append(f"{key} = NULL")
                        elif isinstance(value, bool):
                            set_clauses.append(f"{key} = {str(value).lower()}")
                        elif isinstance(value, str):
                            escaped_value = value.replace("'", "''")
                            set_clauses.append(f"{key} = '{escaped_value}'")
                        else:
                            set_clauses.append(f"{key} = '{value}'")
                
                update_sql = f"UPDATE {FULL_TABLE_NAME} SET {', '.join(set_clauses)} WHERE request_id = '{data['request_id']}'"
                cursor.execute(update_sql)
            else:
                # Build INSERT statement
                columns = list(data.keys())
                values = []
                for v in data.values():
                    if v is None:
                        values.append("NULL")
                    elif isinstance(v, bool):
                        values.append(str(v).lower())
                    elif isinstance(v, str):
                        escaped_value = v.replace("'", "''")
                        values.append(f"'{escaped_value}'")
                    else:
                        values.append(f"'{v}'")
                
                insert_sql = f"INSERT INTO {FULL_TABLE_NAME} ({', '.join(columns)}) VALUES ({', '.join(values)})"
                cursor.execute(insert_sql)
        return True
    except Exception as e:
        st.error(f"Error saving request: {e}")
        return False


def load_request(request_id: str):
    """Load a specific request by ID"""
    try:
        conn = get_connection()
        with conn.cursor() as cursor:
            query = f"SELECT * FROM {FULL_TABLE_NAME} WHERE request_id = '{request_id}'"
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]
            row = cursor.fetchone()
            if row:
                return dict(zip(columns, row))
            return None
    except Exception as e:
        st.warning(f"Could not load request: {e}")
        return None


# Initialize session state
if 'current_request_id' not in st.session_state:
    st.session_state.current_request_id = None
if 'mode' not in st.session_state:
    st.session_state.mode = 'new'  # 'new' or 'edit'


# Header
st.title("📊 Data Feed Intake Form")
st.markdown("Capture metadata for outbound data feeds to vendors.")
st.divider()

# Sidebar - Request Management
with st.sidebar:
    st.header("📋 My Requests")
    
    # For now, use a text input for email (in production, get from auth context)
    user_email = st.text_input("Your Email", placeholder="your.email@ucla.edu")
    
    if st.button("🆕 New Request"):
        st.session_state.current_request_id = None
        st.session_state.mode = 'new'
        st.rerun()
    
    st.divider()
    
    if user_email:
        existing = get_existing_requests(user_email)
        if existing:
            st.markdown("**Previous Submissions:**")
            for req in existing[:10]:  # Show last 10
                status_icon = {"draft": "📝", "submitted": "📤", "complete": "✅"}.get(req.get('status', ''), "❓")
                if st.button(f"{status_icon} {req.get('feed_name', 'Unnamed')}", key=req['request_id']):
                    st.session_state.current_request_id = req['request_id']
                    st.session_state.mode = 'edit'
                    st.rerun()
        else:
            st.info("No previous requests found.")

# Load existing data if editing
existing_data = {}
if st.session_state.mode == 'edit' and st.session_state.current_request_id:
    existing_data = load_request(st.session_state.current_request_id) or {}
    if existing_data.get('status') == 'complete':
        st.warning("⚠️ This request is marked as complete and cannot be edited.")

# Check if form should be read-only
is_readonly = existing_data.get('status') == 'complete'

# Form
st.subheader("Feed Details")

col1, col2 = st.columns(2)

with col1:
    feed_name = st.text_input(
        "Feed Name *",
        value=existing_data.get('feed_name', ''),
        placeholder="e.g., pressganey_patient_survey",
        help="Unique identifier (lowercase, underscores)",
        disabled=is_readonly
    )
    
    source_system = st.text_input(
        "Source System Name *",
        value=existing_data.get('source_system', ''),
        placeholder="e.g., Epic Clarity, SFDC, Manual Upload",
        disabled=is_readonly
    )
    
    vendor_name = st.text_input(
        "Vendor / Destination *",
        value=existing_data.get('vendor_name', ''),
        placeholder="e.g., Press Ganey, Veritas, CMS",
        disabled=is_readonly
    )
    
    target_table = st.text_input(
        "Target Schema/Table Name *",
        value=existing_data.get('target_table', ''),
        placeholder="e.g., silver.pressganey_feed",
        disabled=is_readonly
    )
    
    data_owner_email = st.text_input(
        "Data Owner Email *",
        value=existing_data.get('data_owner_email', ''),
        placeholder="data.owner@ucla.edu",
        disabled=is_readonly
    )

with col2:
    file_name_pattern = st.text_input(
        "File Name Pattern *",
        value=existing_data.get('file_name_pattern', ''),
        placeholder="e.g., UCLA_PressGaney_{YYYYMMDD}.csv",
        help="Use {YYYY}, {MM}, {DD} for date placeholders",
        disabled=is_readonly
    )
    
    landing_zone_path = st.text_input(
        "File Path / Landing Zone *",
        value=existing_data.get('landing_zone_path', ''),
        placeholder="e.g., /mnt/landing/pressganey/",
        disabled=is_readonly
    )
    
    file_format = st.selectbox(
        "File Format *",
        options=["", "CSV", "Pipe-Delimited", "Tab-Delimited", "JSON", "Parquet", "Excel", "Fixed Width"],
        index=["", "CSV", "Pipe-Delimited", "Tab-Delimited", "JSON", "Parquet", "Excel", "Fixed Width"].index(existing_data.get('file_format', '') if existing_data.get('file_format', '') in ["", "CSV", "Pipe-Delimited", "Tab-Delimited", "JSON", "Parquet", "Excel", "Fixed Width"] else ""),
        disabled=is_readonly
    )
    
    # Show delimiter field only for delimited formats
    if file_format in ["CSV", "Pipe-Delimited", "Tab-Delimited"]:
        delimiter_options = {
            "CSV": [","],
            "Pipe-Delimited": ["|"],
            "Tab-Delimited": ["\\t"]
        }
        delimiter = st.selectbox(
            "Column Delimiter *",
            options=delimiter_options.get(file_format, [","]),
            disabled=is_readonly
        )
    else:
        delimiter = existing_data.get('delimiter', '')
    
    header_row = st.radio(
        "Header Row? *",
        options=["Yes", "No"],
        index=0 if existing_data.get('header_row', True) in [True, None] else 1,
        horizontal=True,
        disabled=is_readonly
    )

st.divider()
st.subheader("Schedule & SLA")

col3, col4 = st.columns(2)

with col3:
    schedule_frequency = st.selectbox(
        "Schedule Frequency *",
        options=["", "Daily", "Weekly", "Monthly", "Ad-Hoc"],
        index=["", "Daily", "Weekly", "Monthly", "Ad-Hoc"].index(existing_data.get('schedule_frequency', '') if existing_data.get('schedule_frequency', '') in ["", "Daily", "Weekly", "Monthly", "Ad-Hoc"] else ""),
        disabled=is_readonly
    )
    
    # Parse existing time or default to 6:00 AM
    default_time = time(6, 0)
    if existing_data.get('schedule_time'):
        try:
            parts = existing_data['schedule_time'].split(':')
            default_time = time(int(parts[0]), int(parts[1]))
        except:
            pass
    
    schedule_time = st.time_input(
        "Schedule Time (PST) *",
        value=default_time,
        disabled=is_readonly
    )
    
    load_type = st.radio(
        "Load Type *",
        options=["Full", "Incremental"],
        index=["Full", "Incremental"].index(existing_data.get('load_type', 'Full') if existing_data.get('load_type', 'Full') in ["Full", "Incremental"] else "Full"),
        horizontal=True,
        disabled=is_readonly
    )

with col4:
    # Parse existing SLA time or default to 8:00 AM
    default_sla = time(8, 0)
    if existing_data.get('sla_time'):
        try:
            parts = existing_data['sla_time'].split(':')
            default_sla = time(int(parts[0]), int(parts[1]))
        except:
            pass
    
    sla_time = st.time_input(
        "SLA - File Must Arrive By (PST) *",
        value=default_sla,
        help="Alert if file hasn't arrived by this time",
        disabled=is_readonly
    )

st.divider()
st.subheader("Requestor Information")

col5, col6 = st.columns(2)

with col5:
    requestor_name = st.text_input(
        "Your Name *",
        value=existing_data.get('requestor_name', ''),
        placeholder="Full name",
        disabled=is_readonly
    )

with col6:
    requestor_email = st.text_input(
        "Your Email *",
        value=existing_data.get('requestor_email', user_email or ''),
        placeholder="your.email@ucla.edu",
        disabled=is_readonly
    )

st.divider()

# Notes field
notes = st.text_area(
    "Additional Notes",
    value=existing_data.get('notes', '') or '',
    placeholder="Any additional context or requirements...",
    height=100,
    disabled=is_readonly
)

st.divider()

# Action buttons
if not is_readonly:
    col_btn1, col_btn2, col_btn3, col_btn4 = st.columns([1, 1, 1, 2])
    
    with col_btn1:
        save_draft = st.button("💾 Save Draft", type="secondary")
    
    with col_btn2:
        submit = st.button("📤 Submit", type="primary")
    
    with col_btn3:
        if st.session_state.mode == 'edit' and existing_data.get('status') == 'submitted':
            mark_complete = st.button("✅ Mark Complete")
        else:
            mark_complete = False
    
    # Validation
    required_fields = {
        "Feed Name": feed_name,
        "Source System": source_system,
        "Vendor": vendor_name,
        "Target Table": target_table,
        "Data Owner Email": data_owner_email,
        "File Name Pattern": file_name_pattern,
        "Landing Zone Path": landing_zone_path,
        "File Format": file_format,
        "Schedule Frequency": schedule_frequency,
        "Requestor Name": requestor_name,
        "Requestor Email": requestor_email,
    }
    
    missing = [k for k, v in required_fields.items() if not v]
    
    # Handle save/submit
    if save_draft or submit or mark_complete:
        if submit and missing:
            st.error(f"Please fill in required fields: {', '.join(missing)}")
        else:
            # Determine status
            if mark_complete:
                status = "complete"
            elif submit:
                status = "submitted"
            else:
                status = "draft"
            
            # Build record
            now = datetime.now()
            request_id = existing_data.get('request_id') or generate_request_id()
            
            record = {
                "request_id": request_id,
                "feed_name": feed_name,
                "source_system": source_system,
                "vendor_name": vendor_name,
                "target_table": target_table,
                "data_owner_email": data_owner_email,
                "file_name_pattern": file_name_pattern,
                "landing_zone_path": landing_zone_path,
                "file_format": file_format,
                "delimiter": delimiter if file_format in ["CSV", "Pipe-Delimited", "Tab-Delimited"] else None,
                "header_row": header_row == "Yes",
                "schedule_frequency": schedule_frequency,
                "schedule_time": schedule_time.strftime("%H:%M"),
                "load_type": load_type,
                "sla_time": sla_time.strftime("%H:%M"),
                "requestor_name": requestor_name,
                "requestor_email": requestor_email,
                "notes": notes,
                "status": status,
                "created_at": existing_data.get('created_at') or now.isoformat(),
                "updated_at": now.isoformat(),
            }
            
            is_update = st.session_state.mode == 'edit'
            
            if save_request(record, is_update):
                if status == "draft":
                    st.success(f"✅ Draft saved! Request ID: `{request_id}`")
                elif status == "submitted":
                    st.success(f"✅ Request submitted! Request ID: `{request_id}`")
                    st.info("Next step: Write your SQL code and commit to Git.")
                elif status == "complete":
                    st.success(f"✅ Request marked as complete! Request ID: `{request_id}`")
                    st.balloons()
                
                st.session_state.current_request_id = request_id
                st.session_state.mode = 'edit'
            else:
                st.error("Failed to save. Please try again.")

else:
    st.info("This request is complete and locked for editing.")

# Footer
st.divider()
st.markdown("""
<div style='text-align: center; color: #666;'>
    <small>OHIA Data Architecture & Engineering | Data Feed Intake Form v1.0</small>
</div>
""", unsafe_allow_html=True)
