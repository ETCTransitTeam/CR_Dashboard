import os
import csv
import html
from io import StringIO
from urllib.parse import urlencode
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
import jwt
import time
import bcrypt
import base64
import secrets
import smtplib
import streamlit as st
import snowflake.connector
from dotenv import load_dotenv
from email.mime.text import MIMEText
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
import logging
import streamlit.components.v1 as components

# Set up basic logging (warnings/errors only in production runtime)
logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)
logging.getLogger("botocore").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("boto3").setLevel(logging.WARNING)
logging.getLogger("streamlit").setLevel(logging.WARNING)
load_dotenv()
APP_CONFIG_SCHEMA = os.getenv("APP_CONFIG_SCHEMA", "APP_CONFIG").strip() or "APP_CONFIG"

# Canonical public URL of this Streamlit app (emails, bookmarks). Override via APP_PUBLIC_BASE_URL in .env.
_APP_PUBLIC_BASE_URL = (os.getenv("APP_PUBLIC_BASE_URL") or "http://odcollection.etc-research.com").rstrip("/")


def app_public_url(path_with_query: str = "/") -> str:
    """Build an absolute URL to this app for use outside the browser (e.g. activation emails)."""
    if not path_with_query.startswith("/"):
        path_with_query = "/" + path_with_query
    return f"{_APP_PUBLIC_BASE_URL}{path_with_query}"


def app_public_page_link(page: str, token: str) -> str:
    """Build a magic link with proper query encoding (JWTs in URLs must be encoded or they break)."""
    return f"{_APP_PUBLIC_BASE_URL}/?{urlencode({'page': page, 'token': token})}"


def _smtp_port() -> int:
    try:
        return int(os.getenv("EMAIL_PORT") or "587")
    except (TypeError, ValueError):
        return 587


def _query_param_first(key: str, default=None):
    """Streamlit may return a list for a query key; take the first value for stable token/page reads."""
    v = st.query_params.get(key, default)
    if v is None:
        return default
    if isinstance(v, (list, tuple)):
        return str(v[0]) if v else default
    return str(v)

JWT_SECRET = os.getenv("JWT_SECRET")
JWT_ALGORITHM = os.getenv("JWT_ALGORITHM")
EMAIL_HOST = os.getenv("EMAIL_HOST")
EMAIL_PORT = os.getenv("EMAIL_PORT")
EMAIL_ADDRESS = os.getenv("EMAIL_ADDRESS")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")


# Load the private key from the file
with open("path/to/key.p8", "rb") as key:
    private_key = serialization.load_pem_private_key(
        key.read(),
        password=os.environ["SNOWFLAKE_PASSPHRASE"].encode(),
        backend=default_backend(),
    )

# Serialize the private key to DER format
private_key_bytes = private_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption(),
)

def user_connect_to_snowflake():
    return snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        private_key= private_key_bytes,
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        authenticator="SNOWFLAKE_JWT",
        schema='user',
        role=os.getenv('SNOWFLAKE_ROLE'),
    )

# schema_value = {'TUCSON': 'tucson_bus','TUCSON RAIL': 'tucson_rail','VTA': 'public', 'UTA': 'uta_rail', 'STL':'stl_bus', 'KCATA': 'kcata_bus', 'KCATA RAIL': 'kcata_rail', 'ACTRANSIT': 'actransit_bus', 'SALEM': 'salem_bus'}
# schema_value = {'LACMTA_FEEDER': 'lacmta_feeder_bus', 'SALEM': 'salem_bus', 'ACTRANSIT': 'actransit_bus', 'KCATA': 'kcata_bus', 'KCATA RAIL': 'kcata_rail'}
FRONTEND_HIDDEN_PROJECTS = frozenset({
    "LACMTA_FEEDER",
    "ACTRANSIT",
    "SALEM",
    "PARKCITY",
})


def is_frontend_visible_project(project_name: str) -> bool:
    return (project_name or "").strip() not in FRONTEND_HIDDEN_PROJECTS


def get_projects():
    conn = user_connect_to_snowflake()
    cur = conn.cursor()

    query = f"""
    SELECT PROJECT_NAME, BASE_SCHEMA
    FROM {APP_CONFIG_SCHEMA}.PROJECT_CONFIGS
    WHERE IS_ACTIVE = TRUE
    """

    cur.execute(query)
    projects = dict(cur.fetchall())

    cur.close()
    conn.close()

    return projects


def filter_frontend_projects(projects):
    """Remove projects hidden from dashboard/login dropdowns."""
    if not projects:
        return projects
    return {k: v for k, v in projects.items() if k not in FRONTEND_HIDDEN_PROJECTS}


def get_frontend_projects():
    return filter_frontend_projects(get_projects())


def ensure_client_project_access_table():
    """Create additive access-mapping table used for project-scoped client auth."""
    conn = user_connect_to_snowflake()
    cur = conn.cursor()
    try:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {APP_CONFIG_SCHEMA}")
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {APP_CONFIG_SCHEMA}.CLIENT_PROJECT_ACCESS (
                USER_EMAIL VARCHAR NOT NULL,
                PROJECT_NAME VARCHAR NOT NULL,
                IS_ACTIVE BOOLEAN DEFAULT TRUE,
                CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
            """
        )
    finally:
        cur.close()
        conn.close()


def get_client_allowed_projects(email: str):
    """Return active, frontend-visible project names assigned to a client user email."""
    ensure_client_project_access_table()
    conn = user_connect_to_snowflake()
    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            SELECT PROJECT_NAME
            FROM {APP_CONFIG_SCHEMA}.CLIENT_PROJECT_ACCESS
            WHERE LOWER(USER_EMAIL) = LOWER(%s)
              AND COALESCE(IS_ACTIVE, TRUE) = TRUE
            ORDER BY PROJECT_NAME
            """,
            (email,),
        )
        return [
            row[0]
            for row in (cur.fetchall() or [])
            if row and row[0] and is_frontend_visible_project(row[0])
        ]
    finally:
        cur.close()
        conn.close()


def assign_client_project(email: str, project_name: str) -> None:
    """Upsert one active project mapping for a client email."""
    ensure_client_project_access_table()
    conn = user_connect_to_snowflake()
    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            UPDATE {APP_CONFIG_SCHEMA}.CLIENT_PROJECT_ACCESS
            SET IS_ACTIVE = TRUE,
                UPDATED_AT = CURRENT_TIMESTAMP()
            WHERE LOWER(USER_EMAIL) = LOWER(%s)
              AND UPPER(PROJECT_NAME) = UPPER(%s)
            """,
            (email, project_name),
        )
        if cur.rowcount == 0:
            cur.execute(
                f"""
                INSERT INTO {APP_CONFIG_SCHEMA}.CLIENT_PROJECT_ACCESS (USER_EMAIL, PROJECT_NAME, IS_ACTIVE)
                VALUES (%s, %s, TRUE)
                """,
                (email, project_name),
            )
        conn.commit()
    finally:
        cur.close()
        conn.close()


def assign_client_project_with_status(email: str, project_name: str) -> str:
    """
    Upsert one active project mapping for a client email.
    Returns: 'added' | 'reactivated' | 'already_active'
    """
    ensure_client_project_access_table()
    conn = user_connect_to_snowflake()
    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            SELECT IS_ACTIVE
            FROM {APP_CONFIG_SCHEMA}.CLIENT_PROJECT_ACCESS
            WHERE LOWER(USER_EMAIL) = LOWER(%s)
              AND UPPER(PROJECT_NAME) = UPPER(%s)
            """,
            (email, project_name),
        )
        row = cur.fetchone()
        if row is None:
            cur.execute(
                f"""
                INSERT INTO {APP_CONFIG_SCHEMA}.CLIENT_PROJECT_ACCESS (USER_EMAIL, PROJECT_NAME, IS_ACTIVE)
                VALUES (%s, %s, TRUE)
                """,
                (email, project_name),
            )
            conn.commit()
            return "added"

        is_active = bool(row[0])
        if is_active:
            return "already_active"

        cur.execute(
            f"""
            UPDATE {APP_CONFIG_SCHEMA}.CLIENT_PROJECT_ACCESS
            SET IS_ACTIVE = TRUE,
                UPDATED_AT = CURRENT_TIMESTAMP()
            WHERE LOWER(USER_EMAIL) = LOWER(%s)
              AND UPPER(PROJECT_NAME) = UPPER(%s)
            """,
            (email, project_name),
        )
        conn.commit()
        return "reactivated"
    finally:
        cur.close()
        conn.close()


def get_user_basic_by_email(email: str):
    """Return user basics for an email or None."""
    conn = user_connect_to_snowflake()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT email, username, role, is_active
            FROM user.user_table
            WHERE LOWER(email) = LOWER(%s)
            """,
            (email,),
        )
        row = cur.fetchone()
        if not row:
            return None
        return {
            "email": row[0],
            "username": row[1],
            "role": row[2],
            "is_active": bool(row[3]),
        }
    finally:
        cur.close()
        conn.close()



# Add custom CSS for styling
def add_custom_css():
    st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Poppins:wght@300;400;500;600;700&display=swap');

    /* Global styles */
    .stApp {
        font-family: 'Poppins', sans-serif;
        background: linear-gradient(135deg, #005f82, #007ca5);
        margin: 0;
        min-height: 100vh;
    }
    header, footer, #MainMenu {visibility: hidden;}

    .main .block-container {
        padding: 0 !important;
        margin: 0 !important;
        width: 100% !important;
        max-width: 100% !important;
    }

    /* Proper flex container alignment */
    .auth-container {
        display: flex;
        width: 100%;
        height: 100vh;
        align-items: center;
        justify-content: center;
        gap: 0;
    }
    .stMainBlockContainer {
        padding: 4rem 10rem !important;
    }
    .left-panel {
        flex: 1.2;
        padding-left: 0rem;
        color: white;
        display: flex;
        flex-direction: column;
        justify-content: center;
        position: fixed;
        top: 50%;
        transform: translateY(-50%);
    }

    .left-panel img {
        width: 200px;
        margin-bottom: 25px;
    }

    .left-panel h1 {
        font-size: 38px;
        font-weight: 800;
        margin-bottom: 15px;
    }

    .left-panel p {
        font-size: 18px;
        opacity: 0.9;
        line-height: 1.6;
    }

    .right-panel {
        flex: 1;
        display: flex;
        justify-content: center;
        padding-right: 5rem;
    }
                
    /* Target the right panel container with custom key */
    .st-key-right-panel-box {
        background: white !important;
        padding: 35px !important;
        border-radius: 15px !important;
        box-shadow: 0 10px 30px rgba(0,0,0,0.2) !important;
    }

    /* Ensure the container fills the column */
    .st-key-right-panel-box > div {
        background: white !important;
    }
                
    /* Form card with title */
    .form-box {
        width: 100%;
        max-width: 450px;
        background: rgba(255,255,255,0.2);
        padding: 35px;
        border-radius: 15px;
        backdrop-filter: blur(14px);
        box-shadow: 0 10px 30px rgba(0,0,0,0.2);
    }

    .form-box h2 {
        font-size: 28px;
        font-weight: 700;
        color: #ffffff;
        text-align: center;
        margin-bottom: 10px;
    }
    .form-box .subtitle {
        font-size: 14px;
        text-align: center;
        color: #e0e0e0;
        margin-bottom: 25px;
    }
     
    .stTextInput > label p, .stSelectbox > label p {
       font-weight: 600 !important;
    } 
                
    .stTextInput input, {
        border-radius:8px !important;
    } 
        
    /* Fix dropdown weird split */
    .stSelectbox > div > div {
        border-radius: 8px !important;
        overflow: hidden !important;
    }
    # /*.stSelectbox > div > div > input {
    #     border: none !important;
    # }*/

    /* Inputs */
    input, select, textarea {
        border: none !important;
        border-radius: 12px !important;
        background: rgba(255,255,255,0.85) !important;
        font-size: 15px !important;
        padding: 12px !important;
    }
    input:focus, select:focus {
        outline: none !important;
    }

    /* Buttons */
    .stButton > button {
        width: 100%;
        background: #006894;
        color: white;
        padding: 12px;
        border-radius: 12px;
        border: none;
        font-weight: 600;
        transition: 0.3s ease;
    }
    .stButton > button:hover {
        transform: translateY(-2px);
        box-shadow: 0 6px 20px rgba(102,126,234,0.4);
    }

    .footer {
        text-align: center;
        margin-top: 25px;
        font-size: 13px;
    }
    .footer a {
        font-weight: 600;
    }
    </style>
    """, unsafe_allow_html=True)


_OVERLAY_CLEANUP_JS = """
(function () {
    function scrub(doc) {
        if (!doc || !doc.body) return;
        // Only delete nodes we detached onto document.body.
        // Never touch Streamlit-managed nodes — that causes removeChild crashes.
        Array.from(doc.body.children).forEach(function (element) {
            try {
                var id = element.id || "";
                var cls = element.className || "";
                if (
                    id === "ref-notif-panel" ||
                    id === "ref-user-panel" ||
                    id === "ref-notif-backdrop" ||
                    (typeof cls === "string" && (
                        cls.indexOf("ref-notif-panel") >= 0 ||
                        cls.indexOf("ref-user-panel") >= 0 ||
                        cls.indexOf("ref-notif-backdrop") >= 0
                    ))
                ) {
                    element.remove();
                }
            } catch (error) {}
        });
        // Close any still-managed open menus without deleting them.
        try {
            doc.querySelectorAll(".ref-notif-panel.ref-open, .ref-user-panel.ref-open, .ref-notif-backdrop.ref-open, .ref-bell-wrap.ref-open, .ref-user-menu.ref-open")
                .forEach(function (element) { element.classList.remove("ref-open"); });
        } catch (error) {}
    }
    try {
        scrub(document);
        if (window.parent && window.parent.document) scrub(window.parent.document);
    } catch (error) {}
})();
"""


def _cleanup_detached_dashboard_overlays():
    """Hide leftover RCD menus on auth pages without breaking Streamlit's DOM.

    Scoped to auth layout only — never use naked #ref-notif-panel rules that
    persist across the session and block the RCD notification panel later.
    """
    st.markdown(
        """
        <style>
        /* Only while the login/auth shell is on screen */
        .stApp:has(.auth-container) #ref-notif-panel,
        .stApp:has(.auth-container) #ref-user-panel,
        .stApp:has(.auth-container) #ref-notif-backdrop,
        .stApp:has(.auth-container) .ref-notif-panel,
        .stApp:has(.auth-container) .ref-user-panel,
        .stApp:has(.auth-container) .ref-notif-backdrop {
            display: none !important;
            visibility: hidden !important;
            pointer-events: none !important;
            opacity: 0 !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
    components.html(
        f"<script>{_OVERLAY_CLEANUP_JS}</script>",
        height=0,
        width=0,
    )


def render_auth_layout(content_function, title, subtitle=None, dashboard_type="supervisor"):
    _cleanup_detached_dashboard_overlays()
    add_custom_css()

    subtitle_html = f"<p class='subtitle'>{subtitle}</p>" if subtitle else ""

    # Determine the dashboard title based on the type
    if dashboard_type == "client":
        dashboard_title = "Client"
    else:
        dashboard_title = "Supervisor"

    # Create the two-column layout using Streamlit columns
    col1, col2 = st.columns([1.1, 1])

    # ---- LEFT PANEL ----
    with col1:
        st.markdown(
            f"""
            <div class="left-panel">
                <img src="https://etcinstitute.com/wp-content/uploads/2023/09/ETC-NewLogo-Horizontal-Web.png"
                        alt="ETC Logo" style="width:300px; margin-bottom:20px;"/> <br/>
                <h1>Welcome to {dashboard_title} <br/> Dashboard</h1>
                    <h4 style="margin-bottom:10px">TRANSIT SURVEY</h4>
                    <h5>Origin-Destination Collection Dashboard</h5>
                    <p>725 W. Frontier Lane, Olathe, KS</p>
                    <p> (913) 829-1215</p>
                    <p>info@etcinstitute.com</p>
            </div>
            """,
            unsafe_allow_html=True
        )

    # ---- RIGHT PANEL ----
    with col2:
        with st.container(key="right-panel-box"):
            st.markdown(
                f"""
                <h2 style="color: rgb(29, 39, 96); padding-top:0"; font-weight: 700>{title}</h2>
                {subtitle_html}
                """,
                unsafe_allow_html=True
            )

            # The actual form content will be rendered here
            content_function()

            # Footer section
            st.markdown('<hr style="border: 0.2px solid black; margin-top: 24px; margin-bottom: 0;">', unsafe_allow_html=True)
            st.markdown(
                """
                <div class="footer">
                    <div class="social-links">
                        <a href="https://www.facebook.com/etcinstitute/">Facebook</a> |
                        <a href="https://twitter.com/EtcInstitute">Twitter</a> |
                        <a href="https://www.linkedin.com/company/etc-institute/">LinkedIn</a> |
                        <a href="https://www.instagram.com/etcinstitute/">Instagram</a>
                    </div>
                    <p>ETC Institute © 2025 All Rights Reserved.</p>
                </div>
                """,
                unsafe_allow_html=True
            )


def send_activation_email(email, activation_token):
    """Send an account activation email with a secure token using HTML format."""
    activation_link = app_public_page_link("activate", activation_token)

    subject = "Activate Your Account - ETC Institute"
    
    # HTML email body
    body = f"""
    <html>
    <head>
         <style>
            .container {{
                font-family: Arial, sans-serif;
                text-align: center;
                padding: 20px;
                background-color: #f4f4f4;
            }}
            .email-box {{
                background: white;
                padding: 30px;
                border-radius: 15px;
                box-shadow: 0 10px 30px rgba(0,0,0,0.1);
                max-width: 500px;
                margin: auto;
            }}
            .logo {{
                color: #667eea;
                font-size: 24px;
                font-weight: bold;
                margin-bottom: 20px;
            }}
            .btn {{
                display: inline-block;
                padding: 12px 30px;
                margin: 20px 0;
                font-size: 16px;
                color: #FFF;
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                text-decoration: none;
                border-radius: 8px;
                font-weight: bold;
            }}
            .footer {{
                margin-top: 30px;
                font-size: 12px;
                color: #666;
                border-top: 1px solid #eee;
                padding-top: 20px;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="email-box">
                <div class="logo">ETC Institute</div>
                <h2>Welcome to ETC Institute!</h2>
                <p>Thank you for signing up for our analytics platform. Please activate your account by clicking the button below:</p>
                <a class="btn" href="{activation_link}" style="color: #fff">Activate My Account</a>
                <p>If the button above doesn't work, you can also use this link:</p>
                <p><a href="{activation_link}">{activation_link}</a></p>
                <div class="footer">
                    <p>725 W. Frontier Lane, Olathe, KS | (913) 829-1215</p>
                    <p>Helping Organizations Make Better Decisions Since 1982</p>
                </div>
            </div>
        </div>
    </body>
    </html>
    """

    try:
        msg = MIMEMultipart()
        msg['From'] = EMAIL_ADDRESS
        msg['To'] = email
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'html'))

        with smtplib.SMTP(EMAIL_HOST, _smtp_port()) as server:
            server.starttls()
            server.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
            server.sendmail(EMAIL_ADDRESS, email, msg.as_string())

        logger.info("Activation email sent to %s", email)
        return True
    except Exception as e:
        logger.exception("Failed to send activation email to %s: %s", email, e)
        return False

def create_new_user(email, username, password, role):
    """Create a new user in Snowflake (admin-only)."""
    conn = user_connect_to_snowflake()
    cursor = conn.cursor()

    cursor.execute("SELECT email FROM user.user_table WHERE email = %s", (email,))
    if cursor.fetchone():
        st.error("User already exists!")
        cursor.close()
        conn.close()
        return False

    hashed_password = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt())
    encoded_password = base64.b64encode(hashed_password).decode('utf-8')
    activation_token = secrets.token_urlsafe(32)

    insert_query = """
        INSERT INTO user.user_table (email, username, password, role, is_active, activation_token) 
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    try:
        cursor.execute(insert_query, (email, username, encoded_password, role, False, activation_token))
        conn.commit()
        if send_activation_email(email, activation_token):
            st.success(f"User {username} created successfully! Activation email sent.")
        else:
            st.success(f"User {username} created, but the activation email failed to send. Check logs and SMTP settings.")
        return True
    except Exception as e:
        st.error(f"Failed to create user: {e}")
        return False
    finally:
        cursor.close()
        conn.close()

def create_new_user_page():
    """Public staff registration (Review Cycle cleaning team); role defaults to CLEANING."""
    def create_user_content():
        st.caption(
            "Create a staff account for the Review Cycle workflow. "
            "An activation email is sent before you can sign in."
        )
        with st.form(key="create_new_user_form"):
            col1, col2 = st.columns(2)
            with col1:
                username = st.text_input("Username", placeholder="Enter username")
            with col2:
                email = st.text_input("Email", placeholder="Enter work email")
            col3, col4 = st.columns(2)
            with col3:
                password = st.text_input("Password", type="password", placeholder="Enter password")
            with col4:
                confirm_password = st.text_input("Confirm Password", type="password", placeholder="Re-enter password")

            if st.form_submit_button("Create staff account", type="primary", use_container_width=True):
                if not all([username, email, password, confirm_password]):
                    st.error("Please complete all fields.")
                elif password != confirm_password:
                    st.error("Passwords do not match.")
                else:
                    try:
                        if create_new_user(email, username, password, "CLEANING"):
                            st.success(f"User **{username}** created successfully! Activation email sent.")
                            time.sleep(2)
                            st.markdown('<meta http-equiv="refresh" content="0;url=/?page=create_user">', unsafe_allow_html=True)
                        else:
                            st.error("Could not create the user. Please try again.")
                    except Exception as e:
                        st.error(f"Error: {str(e)}")

        st.markdown("""
            <style>
            .auth-link {
                text-align: center;
            }
            .auth-link a:hover {
                text-decoration: underline;
            }
            </style>
            """, unsafe_allow_html=True)

        st.markdown("""
            <div class="auth-link">
                Already have an account? <a href="/?page=login">Login here</a>
            </div>
            """, unsafe_allow_html=True)

    render_auth_layout(create_user_content, "Staff Registration", "Request access to ETC Institute staff tools")

def register_new_user(email, username, password, role):
    conn = user_connect_to_snowflake()
    cursor = conn.cursor()

    cursor.execute("SELECT email FROM user.user_table WHERE email = %s", (email,))
    if cursor.fetchone():
        st.error('User already exists!')
        return False, False

    hashed_password = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt())
    encoded_password = base64.b64encode(hashed_password).decode('utf-8')
    activation_token = secrets.token_urlsafe(32)

    insert_query = """
        INSERT INTO user.user_table (email, username, password, role, is_active, activation_token) 
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    cursor.execute(insert_query, (email, username, encoded_password, role, False, activation_token))
    conn.commit()
    cursor.close()
    conn.close()

    email_sent = send_activation_email(email, activation_token)
    if not email_sent:
        st.warning(
            "Your account was created, but the activation email could not be sent. "
            "Please contact support or try forgot-password after verifying your address."
        )
    return True, email_sent

def register_page():
    """Displays a registration form and handles user registration."""
    def register_content():
        with st.form(key="register_form"):
            # First row - Username and Email
            username = st.text_input("Username", placeholder="Choose a username")
            email = st.text_input("Email", placeholder="Enter your email address")
            password1 = st.text_input("Password", type="password", placeholder="Create a password")
            password2 = st.text_input("Confirm Password", type="password", placeholder="Re-enter your password")

            if st.form_submit_button("Create Account", type='primary' , use_container_width=True):
                if not username or not email or not password1 or not password2:
                    st.error("Please fill in all fields.")
                elif password1 != password2:
                    st.error("Passwords do not match.")
                else:
                    created, email_sent = register_new_user(email, username, password1, 'CLIENT')
                    if created and email_sent:
                        st.success("Registration successful! Check your email for activation link.")
                        time.sleep(2)
                        st.markdown(f'<meta http-equiv="refresh" content="0;url=/?page=login">', unsafe_allow_html=True)
                    elif created:
                        st.info("Account was created. If email delivery failed, use the message above and contact support.")

        st.markdown("""
            <style>
            .auth-link {
                text-align: center;
            }
            .auth-link a:hover {
                text-decoration: underline;
            }
            </style>
            """, unsafe_allow_html=True)

        st.markdown("""
            <div class="auth-link">
                Already have an account? <a href="/?page=login">Login here</a>
            </div>
            """, unsafe_allow_html=True)

    
    render_auth_layout(register_content, "Create Account", "Join ETC Institute's analytics platform", dashboard_type="client")


def _resolve_signup_project_name():
    raw_project = _query_param_first("project", "")
    project = (raw_project or "").strip()
    if not project:
        return None

    projects = get_frontend_projects()
    for name in projects.keys():
        if name.strip().lower() == project.lower():
            return name
    return None


def client_signup_page():
    """Project-scoped signup: creates CLIENT user and assigns that project access."""
    resolved_project = _resolve_signup_project_name()
    requested_project = (_query_param_first("project", "") or "").strip()

    def register_content():
        if not requested_project:
            st.error("Missing project in signup URL.")
            return
        if not resolved_project:
            st.error("Invalid or inactive project in signup URL.")
            return

        st.info(f"Project access for this signup: {resolved_project}")
        with st.form(key="project_register_form"):
            username = st.text_input("Username", placeholder="Choose a username")
            email = st.text_input("Email", placeholder="Enter your email address")
            password1 = st.text_input("Password", type="password", placeholder="Create a password")
            password2 = st.text_input("Confirm Password", type="password", placeholder="Re-enter your password")

            if st.form_submit_button("Create Account", type='primary', use_container_width=True):
                if not username or not email or not password1 or not password2:
                    st.error("Please fill in all fields.")
                elif password1 != password2:
                    st.error("Passwords do not match.")
                else:
                    existing_user = get_user_basic_by_email(email)
                    if existing_user:
                        existing_role = str(existing_user.get("role", "")).upper()
                        if existing_role != "CLIENT":
                            st.error(
                                "This email already exists as a non-client account. "
                                "Please use a different email or contact administrator."
                            )
                            return

                        status = assign_client_project_with_status(email, resolved_project)
                        if status == "already_active":
                            st.info(
                                f"User already exists and already has access to {resolved_project}."
                            )
                        else:
                            st.success(
                                f"User already exists. Project access added for {resolved_project}."
                            )

                        if not existing_user.get("is_active", False):
                            st.warning(
                                "This account is not active yet. Please activate it from the email "
                                "or use forgot password to get a fresh link."
                            )
                        else:
                            time.sleep(1)
                            st.markdown(
                                f'<meta http-equiv="refresh" content="0;url=/?page=client_login">',
                                unsafe_allow_html=True,
                            )
                        return

                    created, email_sent = register_new_user(email, username, password1, 'CLIENT')
                    if created:
                        assign_client_project_with_status(email, resolved_project)
                    if created and email_sent:
                        st.success("Registration successful! Check your email for activation link.")
                        time.sleep(2)
                        st.markdown(f'<meta http-equiv="refresh" content="0;url=/?page=client_login">', unsafe_allow_html=True)
                    elif created:
                        st.info("Account created. If activation email failed, contact support.")

        st.markdown(
            """
            <div class="auth-link">
                Already have an account? <a href="/?page=client_login">Login here</a>
            </div>
            """,
            unsafe_allow_html=True,
        )

    render_auth_layout(
        register_content,
        "Client Signup",
        "Create your account for your assigned project.",
        dashboard_type="client",
    )


def activate_account():
    """Activate the user account based on the verification token."""
    def activate_content():
        token = _query_param_first("token")

        if not token:
            st.error("Invalid activation link.")
            return

        conn = user_connect_to_snowflake()
        cursor = conn.cursor()
        
        cursor.execute("SELECT email, is_active FROM user.user_table WHERE activation_token = %s", (token,))
        user_record = cursor.fetchone()

        if not user_record:
            st.error("Invalid or expired activation token.")
            cursor.close()
            conn.close()
            return

        email = user_record[0]
        is_active = bool(user_record[1]) if len(user_record) > 1 else False

        if is_active:
            cursor.close()
            conn.close()
            st.success("Your account is already activated. Please log in.")
            st.markdown(f'<meta http-equiv="refresh" content="1;url=/?page=login">', unsafe_allow_html=True)
            return

        st.info("Click Activate to complete your account activation.")
        if st.button("Activate Account", type="primary", use_container_width=True):
            cursor.execute(
                """
                UPDATE user.user_table
                SET is_active = %s, activation_token = NULL
                WHERE email = %s AND activation_token = %s
                """,
                (True, email, token),
            )
            conn.commit()
            cursor.close()
            conn.close()

            if cursor.rowcount == 0:
                st.error("This activation link was already used or is no longer valid.")
                return

            st.success("🎉 Your account has been activated! You can now log in.")
            st.info("Redirecting to login page...")
            st.markdown(f'<meta http-equiv="refresh" content="2;url=/?page=login">', unsafe_allow_html=True)
            return

        cursor.close()
        conn.close()
    
    render_auth_layout(activate_content, "Account Activation", "Your account is being activated...")

def generate_jwt(email, username, role):
    payload = {
        "email": email,
        "username": username,
        "role": role,
        "exp": datetime.utcnow() + timedelta(hours=1)
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)

def store_user_in_session(user_data):
    st.session_state["logged_in"] = True
    st.session_state["user"] = user_data
    st.session_state["token"] = generate_jwt(user_data["email"], user_data["username"], user_data["role"])

def check_user_login(email, password):
    conn = user_connect_to_snowflake()
    cursor = conn.cursor()

    query = """
    SELECT email, username, password, role, is_active
    FROM user.user_table
    WHERE email = %s
    """
    cursor.execute(query, (email,))
    user = cursor.fetchone()
    
    if user:
        stored_hashed_password = base64.b64decode(user[2])
        is_active = user[4]

        if is_active:
            if bcrypt.checkpw(password.encode('utf-8'), stored_hashed_password):
                return {"email": user[0], "username": user[1], "role": user[3]}
            else:
                return None
        else:
            return "inactive"
    else:
        return None

def is_super_admin(email):
    """Check if the email belongs to a super admin."""
    return email.lower() in [admin_email.lower() for admin_email in SUPER_ADMIN_EMAILS]


def can_access_survey_assignment_manager(email: str, role: str | None = None) -> bool:
    """Survey Assignment Manager: super admins and ADMIN role only (not CLIENT or USER)."""
    if is_super_admin(email):
        return True
    return str(role or "").upper() == "ADMIN"


PORTAL_OD = "od"
PORTAL_REVIEW_CYCLE = "review_cycle"
PORTAL_SAM = "sam"

VALID_STAFF_ROLES = {"USER", "ADMIN", "CLEANING", "CLIENT"}


def allowed_portals(email: str, role: str | None = None) -> list[str]:
    """Portals a user may open after login."""
    role_u = str(role or "").upper()
    if is_super_admin(email):
        return [PORTAL_OD, PORTAL_REVIEW_CYCLE, PORTAL_SAM]
    if role_u == "CLIENT":
        return [PORTAL_OD]
    if role_u == "CLEANING":
        return [PORTAL_REVIEW_CYCLE]
    if role_u == "ADMIN":
        return [PORTAL_OD, PORTAL_REVIEW_CYCLE, PORTAL_SAM]
    if role_u == "USER":
        return [PORTAL_REVIEW_CYCLE]
    return [PORTAL_OD]


def od_role_to_rcd_role(email: str, role: str | None = None) -> str:
    """Map unified OD role to Review Cycle internal role."""
    if is_super_admin(email):
        return "admin"
    role_u = str(role or "").upper()
    if role_u == "ADMIN":
        return "manager"
    if role_u == "CLEANING":
        return "cleaning"
    if role_u == "USER":
        return "field"
    raise ValueError(f"No Review Cycle access for role {role_u or 'unknown'}")


def _portal_entry_page(portal: str) -> str:
    if portal == PORTAL_OD:
        return "od_project_select"
    if portal == PORTAL_SAM:
        return "field_assignments"
    if portal == PORTAL_REVIEW_CYCLE:
        return "review_cycle"
    return "portal_select"


def page_after_login(email: str, role: str | None = None) -> str:
    """First page to open after successful staff login."""
    portals = allowed_portals(email, role)
    if len(portals) == 1:
        return _portal_entry_page(portals[0])
    return "portal_select"


def login(client_mode: bool = False):
    """Displays a login form and handles authentication."""
    def login_content():

        schema_value = get_frontend_projects()
        project_names = list(schema_value.keys())
        with st.form(key="login_form"):
            email = st.text_input("Email", placeholder="Enter your email address")
            password = st.text_input("Password", type="password", placeholder="Enter your password")
            st.markdown(
                '<div style="text-align: right; margin-bottom: 8px;">'
                '<a href="/?page=forgot_password" style="color: red; text-decoration: underline;">Forgot Password?</a>'
                '</div>',
                unsafe_allow_html=True
            )
            login_submit = st.form_submit_button("Login", type="primary", use_container_width=True)

            if login_submit:
                user = check_user_login(email, password)
                if user == "inactive":
                    st.error("Your account is not active. Please verify your email before logging in.")
                elif user:
                    user_role = str(user.get("role", "")).upper()
                    if client_mode and user_role != "CLIENT":
                        st.session_state.pop("user", None)
                        st.session_state.pop("logged_in", None)
                        st.session_state.pop("token", None)
                        st.session_state.pop("jwt_token", None)
                        st.session_state.pop("selected_project", None)
                        st.session_state.pop("schema", None)
                        st.error("This login page is for client accounts only.")
                        return

                    if user_role == "CLIENT":
                        allowed_projects = get_client_allowed_projects(user["email"])
                        if allowed_projects:
                            valid_allowed = [p for p in allowed_projects if p in schema_value]
                            if not valid_allowed:
                                st.error(
                                    "Your assigned projects are no longer available in the client portal. "
                                    "Please contact your administrator."
                                )
                                return
                            if len(valid_allowed) == 1:
                                selected_project = valid_allowed[0]
                                store_user_in_session(user)
                                st.session_state["logged_in"] = True
                                st.session_state["jwt_token"] = generate_jwt(
                                    user["email"], user["username"], user["role"]
                                )
                                st.session_state["login_redirect_page"] = "client_login"
                                st.session_state["selected_project"] = selected_project
                                st.session_state["schema"] = schema_value[selected_project]
                                st.query_params["logged_in"] = "true"
                                st.query_params["page"] = "main"
                                st.rerun()
                                return
                            store_user_in_session(user)
                            st.session_state["logged_in"] = True
                            st.session_state["jwt_token"] = generate_jwt(
                                user["email"], user["username"], user["role"]
                            )
                            st.session_state["login_redirect_page"] = "client_login"
                            st.session_state["client_candidate_projects"] = valid_allowed
                            st.query_params["logged_in"] = "true"
                            st.query_params["page"] = "client_project_select"
                            st.rerun()
                            return
                        if client_mode:
                            store_user_in_session(user)
                            st.session_state["logged_in"] = True
                            st.session_state["jwt_token"] = generate_jwt(
                                user["email"], user["username"], user["role"]
                            )
                            st.session_state["login_redirect_page"] = "client_login"
                            st.session_state["client_candidate_projects"] = project_names
                            st.query_params["logged_in"] = "true"
                            st.query_params["page"] = "client_project_select"
                            st.rerun()
                            return

                    store_user_in_session(user)
                    st.success(f"Welcome {user['username']}!")
                    jwt_token = generate_jwt(user["email"], user["username"], user["role"])
                    st.session_state["logged_in"] = True
                    st.session_state["jwt_token"] = jwt_token
                    st.session_state["login_redirect_page"] = "client_login" if client_mode else "login"
                    st.session_state.pop("selected_project", None)
                    st.session_state.pop("schema", None)
                    st.query_params["logged_in"] = "true"
                    st.query_params["page"] = page_after_login(user["email"], user_role)
                    st.rerun()
                else:
                    st.error("Incorrect email or password")

        st.markdown(
            '''
            <div style="
                background: #ffffff;
                padding: 5px;
                border-radius: 10px;
                text-align: center;
                margin: 10px 0;
                border: 3px solid transparent;
                background-clip: padding-box;
                position: relative;
                background: linear-gradient(white, white), 
                            linear-gradient(135deg, #ff6b6b, #4ecdc4, #45b7d1, #96ceb4, #ffeaa7);
                background-origin: padding-box, border-box;
                background-clip: padding-box, border-box;
            ">
                <h5 style="color: #2d3436;padding-bottom:10px">Need Access?</h5>
                <p style="color: #636e72; margin: 0; font-size: 14px;">
                    Staff member? <a href="/?page=create_user">Register here</a>.
                    Client project signup uses your project link from your administrator.
                </p>
            </div>
            ''',
            unsafe_allow_html=True
        )

    page_title = "Client Login" if client_mode else "Login"
    subtitle = "Login to your assigned project account" if client_mode else ""
    render_auth_layout(login_content, page_title, subtitle)

def logout():
    redirect_page = st.session_state.get("login_redirect_page", "login")
    # Scrub detached RCD overlays before the login page paints.
    _cleanup_detached_dashboard_overlays()
    st.session_state.clear()
    st.success("Logged out successfully!")
    st.query_params["page"] = redirect_page
    st.rerun()


def _portal_display_name(user: dict) -> str:
    return str(user.get("username") or user.get("name") or user.get("email") or "User").strip() or "User"


def _portal_role_label(email: str, role: str) -> str:
    if is_super_admin(email):
        return "Super Admin"
    role_u = str(role or "").upper()
    labels = {
        "ADMIN": "Admin",
        "USER": "User",
        "CLEANING": "Cleaning Team",
        "CLIENT": "Client",
        "MANAGER": "Manager",
    }
    return labels.get(role_u, role_u.replace("_", " ").title() or "User")


def _portal_initials(name: str) -> str:
    parts = [p for p in str(name or "").strip().split() if p]
    if len(parts) >= 2:
        return (parts[0][0] + parts[1][0]).upper()
    text = "".join(parts) or "?"
    return text[:2].upper()


def _portal_check_icon(color: str) -> str:
    return (
        f'<span class="etc-hub-check" style="color:{html.escape(color)}">'
        '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" aria-hidden="true">'
        '<circle cx="12" cy="12" r="10" fill="currentColor" opacity="0.15"/>'
        '<path d="M7.5 12.5l3 3 6-6.5" stroke="currentColor" stroke-width="2.2" '
        'stroke-linecap="round" stroke-linejoin="round"/>'
        "</svg></span>"
    )


def _portal_feature_row(color: str, text: str) -> str:
    """Single feature row: check SVG + wording on one line (Streamlit-safe)."""
    return (
        '<div class="etc-hub-feature-row">'
        f"{_portal_check_icon(color)}"
        f'<span class="etc-hub-feature-text">{html.escape(str(text))}</span>'
        "</div>"
    )


def _portal_hub_styles() -> None:
    """Styles for the ETC OD Collection Platform portal chooser."""
    st.markdown(
        """
        <style>
        section.main > div { background: #f4f7fb !important; }
        .stApp { background: #f4f7fb !important; }
        .main .block-container,
        .stMainBlockContainer,
        div[data-testid="stMainBlockContainer"],
        section.main .block-container,
        .stAppViewContainer .main .block-container {
            width: 100% !important;
            max-width: 1440px !important;
            margin: 0 auto !important;
            padding: 1.25rem 2rem 2rem !important;
            padding-left: 2rem !important;
            padding-right: 2rem !important;
        }
        /* Comfortable vertical rhythm — not cramped */
        div[data-testid="stMainBlockContainer"] > div[data-testid="stVerticalBlock"] {
            gap: 1rem !important;
        }
        div[data-testid="stMainBlockContainer"] [data-testid="stVerticalBlockBorderWrapper"] {
            margin-bottom: 0 !important;
        }

        .etc-hub-topbar {
            display: flex;
            align-items: center;
            justify-content: space-between;
            gap: 16px;
            background: #ffffff;
            border: 1px solid #e2e8f0;
            border-radius: 14px;
            padding: 14px 20px;
            margin-bottom: 4px;
            box-shadow: 0 4px 14px rgba(15, 23, 42, 0.04);
        }
        .etc-hub-brand {
            display: flex;
            align-items: center;
            gap: 12px;
            min-width: 0;
        }
        .etc-hub-mark {
            width: 40px; height: 40px; border-radius: 11px;
            background: linear-gradient(145deg, #1d4ed8 0%, #2563eb 100%);
            color: #fff; display: inline-flex; align-items: center; justify-content: center;
            font-size: 12px; font-weight: 800; letter-spacing: 0.02em; flex-shrink: 0;
        }
        .etc-hub-brand-title {
            margin: 0; font-size: 17px; font-weight: 700; color: #0f2744; line-height: 1.2;
        }
        .etc-hub-userchip {
            display: inline-flex; align-items: center; gap: 10px; min-width: 0;
        }
        .etc-hub-avatar {
            width: 38px; height: 38px; border-radius: 50%;
            background: #2563eb; color: #fff; display: inline-flex;
            align-items: center; justify-content: center;
            font-size: 12px; font-weight: 700; flex-shrink: 0;
        }
        .etc-hub-user-meta { min-width: 0; line-height: 1.2; }
        .etc-hub-username {
            margin: 0; font-size: 14px; font-weight: 700; color: #0f172a;
            white-space: nowrap; overflow: hidden; text-overflow: ellipsis; max-width: 180px;
        }
        .etc-hub-userrole {
            margin: 2px 0 0; font-size: 12px; color: #64748b; font-weight: 500;
        }
        .etc-hub-chevron { color: #94a3b8; font-size: 12px; margin-left: 2px; }

        [data-testid="stHorizontalBlock"]:has(.etc-hub-banner) {
            background:
                radial-gradient(circle at 12% 20%, rgba(96,165,250,0.22), transparent 42%),
                radial-gradient(circle at 88% 30%, rgba(167,139,250,0.18), transparent 40%),
                linear-gradient(135deg, #eef5ff 0%, #f5f8ff 55%, #eef8ff 100%);
            border: 1px solid #dbe7f8;
            border-radius: 18px;
            padding: 18px 14px 18px 22px !important;
            margin: 4px 0 8px;
            align-items: center !important;
            gap: 0 !important;
        }
        [data-testid="stHorizontalBlock"]:has(.etc-hub-banner) > div[data-testid="stColumn"] > div[data-testid="stVerticalBlock"] {
            gap: 0 !important;
            justify-content: center !important;
        }
        [data-testid="stHorizontalBlock"]:has(.etc-hub-banner) [data-testid="stMarkdownContainer"],
        [data-testid="stHorizontalBlock"]:has(.etc-hub-banner) .stMarkdown {
            margin: 0 !important;
            padding: 0 !important;
        }
        /* Sign out column: pin button to the far right of the banner */
        [data-testid="stHorizontalBlock"]:has(.etc-hub-banner) > div[data-testid="stColumn"]:last-child {
            border-left: 1px solid #c5d5eb !important;
            padding: 0 0 0 16px !important;
            margin: 0 !important;
            display: flex !important;
            align-items: center !important;
            justify-content: flex-end !important;
        }
        [data-testid="stHorizontalBlock"]:has(.etc-hub-banner) > div[data-testid="stColumn"]:last-child > div,
        [data-testid="stHorizontalBlock"]:has(.etc-hub-banner) > div[data-testid="stColumn"]:last-child [data-testid="stVerticalBlock"] {
            width: 100% !important;
            align-items: flex-end !important;
            justify-content: center !important;
            gap: 0 !important;
        }
        .etc-hub-banner {
            display: flex !important;
            flex-direction: row !important;
            align-items: center !important;
            justify-content: space-between;
            gap: 18px;
            background: transparent; border: 0; border-radius: 0;
            padding: 0 !important; margin: 0 !important;
        }
        .etc-hub-banner-left {
            display: flex !important;
            flex-direction: row !important;
            flex-wrap: nowrap !important;
            align-items: center !important;
            gap: 16px !important;
            min-width: 0;
            width: 100%;
        }
        .etc-hub-banner-icon {
            width: 58px; height: 58px; border-radius: 16px;
            background: rgba(255,255,255,0.75); border: 1px solid #dbeafe;
            display: inline-flex !important; align-items: center; justify-content: center;
            flex: 0 0 58px !important;
        }
        .etc-hub-banner-icon svg {
            display: block !important; width: 34px; height: 34px; max-width: 34px !important;
        }
        .etc-hub-banner-copy {
            display: flex !important;
            flex-direction: column !important;
            justify-content: center !important;
            min-width: 0;
            flex: 1 1 auto !important;
        }
        .etc-hub-banner-title {
            margin: 0 !important; padding: 0 !important;
            font-size: 28px; font-weight: 800; color: #0f172a;
            letter-spacing: -0.02em; line-height: 1.15;
            display: block !important;
        }
        .etc-hub-banner-sub {
            margin: 6px 0 0 !important; padding: 0 !important;
            font-size: 14px; color: #64748b; line-height: 1.35;
            display: block !important;
        }
        .etc-hub-banner p,
        .etc-hub-banner-left p,
        .etc-hub-banner-copy p {
            display: contents !important;
            margin: 0 !important;
        }

        .etc-hub-section-head {
            display: flex !important;
            flex-direction: row !important;
            flex-wrap: nowrap !important;
            align-items: center !important;
            gap: 14px !important;
            margin: 10px 0 14px !important;
            width: 100%;
        }
        .etc-hub-section-icon {
            width: 34px; height: 34px; border-radius: 10px;
            background: #eff6ff; color: #2563eb;
            display: inline-flex !important; align-items: center; justify-content: center;
            flex: 0 0 34px !important;
        }
        .etc-hub-section-icon svg {
            display: block !important; width: 16px; height: 16px; max-width: 16px !important;
        }
        .etc-hub-section-copy {
            display: flex !important;
            flex-direction: column !important;
            justify-content: center !important;
            min-width: 0;
            flex: 1 1 auto !important;
        }
        .etc-hub-section-title {
            margin: 0 !important; padding: 0 !important;
            font-size: 18px; font-weight: 750; color: #0f172a; line-height: 1.2;
            display: block !important;
        }
        .etc-hub-section-sub {
            margin: 2px 0 0 !important; padding: 0 !important;
            font-size: 13px; color: #64748b; line-height: 1.3;
            display: block !important;
        }
        .etc-hub-section-head p,
        .etc-hub-section-copy p {
            display: contents !important;
        }

        .etc-hub-card {
            background: transparent;
            border: 0;
            border-radius: 0;
            padding: 0;
            min-height: 0 !important;
            height: auto !important;
            box-shadow: none;
            display: flex; flex-direction: column; gap: 14px;
            margin-bottom: 0;
        }
        /* Streamlit can't nest buttons in HTML — wrap column content so Open buttons sit inside the card */
        div[data-testid="stHorizontalBlock"]:has(.etc-hub-card) > div[data-testid="stColumn"] > div[data-testid="stVerticalBlock"] {
            background: #ffffff !important;
            border: 1px solid #e2e8f0 !important;
            border-radius: 18px !important;
            padding: 24px 22px 20px !important;
            box-shadow: 0 10px 24px rgba(15, 23, 42, 0.05) !important;
            gap: 0.75rem !important;
            height: 100%;
        }
        .etc-hub-card-top {
            display: flex !important;
            flex-direction: row !important;
            align-items: center !important;
            gap: 14px !important;
            width: 100%;
            margin-bottom: 2px;
        }
        .etc-hub-card-icon {
            width: 48px; height: 48px; border-radius: 14px;
            display: inline-flex !important; align-items: center; justify-content: center;
            flex-shrink: 0 !important;
        }
        .etc-hub-card-icon svg {
            display: block !important; width: 22px; height: 22px;
            max-width: 22px !important; flex-shrink: 0;
        }
        .etc-hub-card-icon.od { background: #eff6ff; color: #2563eb; }
        .etc-hub-card-icon.rc { background: #ecfdf5; color: #0f766e; }
        .etc-hub-card-icon.sam { background: #f5f3ff; color: #7c3aed; }
        .etc-hub-card-title {
            margin: 0 !important; font-size: 18px; font-weight: 750;
            color: #0f172a; line-height: 1.25;
        }
        .etc-hub-card > p, .etc-hub-card-desc {
            margin: 0 !important; font-size: 14px; line-height: 1.5; color: #64748b;
        }
        .etc-hub-features {
            list-style: none; margin: 6px 0 4px !important; padding: 0 !important;
            display: flex !important; flex-direction: column !important;
            gap: 14px !important; flex: 0 0 auto !important;
        }
        .etc-hub-feature-row {
            display: flex !important;
            flex-direction: row !important;
            flex-wrap: nowrap !important;
            align-items: center !important;
            gap: 12px !important;
            width: 100%;
            font-size: 14px; color: #334155; line-height: 1.4;
        }
        .etc-hub-check {
            width: 20px; height: 20px; border-radius: 50%;
            display: inline-flex !important; align-items: center; justify-content: center;
            flex: 0 0 20px !important; margin: 0 !important; padding: 0 !important;
            vertical-align: middle;
        }
        .etc-hub-check svg {
            display: block !important; width: 14px !important; height: 14px !important;
            max-width: 14px !important; flex-shrink: 0;
        }
        .etc-hub-feature-text {
            display: inline !important;
            margin: 0 !important; padding: 0 !important;
            white-space: normal;
            flex: 1 1 auto !important;
            min-width: 0;
        }
        /* Streamlit may wrap SVG/text in <p>; unwrap only inside feature/title rows */
        .etc-hub-feature-row p,
        .etc-hub-card-top p,
        .etc-hub-check p {
            display: contents !important;
            margin: 0 !important;
        }

        .etc-hub-footer {
            display: flex; align-items: center; justify-content: space-between;
            gap: 16px; flex-wrap: wrap;
            margin-top: 8px; padding: 18px 4px 0;
            border-top: 1px solid #e2e8f0; color: #64748b; font-size: 13px;
        }
        .etc-hub-footer-left, .etc-hub-footer-right {
            display: inline-flex; align-items: center; gap: 12px;
        }
        .etc-hub-footer-sep { color: #cbd5e1; }
        .etc-hub-version {
            display: inline-block; padding: 3px 9px; border-radius: 999px;
            background: #eef5ff; border: 1px solid #dbeafe; color: #3b82f6;
            font-size: 12px; font-weight: 650;
        }

        div[class*="st-key-portal_hub_sign_out"] {
            display: block !important;
            width: 100% !important;
            margin: 0 !important;
            padding: 0 !important;
        }
        div[class*="st-key-portal_hub_sign_out"] > div {
            width: 100% !important;
            margin: 0 !important;
        }
        div[class*="st-key-portal_hub_sign_out"] button {
            background: #ffffff !important; color: #0f172a !important;
            border: 1px solid #cbd5e1 !important; border-radius: 10px !important;
            font-weight: 700 !important; font-size: 13px !important;
            box-shadow: none !important;
            height: 38px !important;
            min-height: 38px !important;
            padding: 0 12px !important;
            width: 100% !important;
            gap: 6px !important;
            margin: 0 !important;
            float: none !important;
        }
        div[class*="st-key-portal_hub_sign_out"] button p,
        div[class*="st-key-portal_hub_sign_out"] button span {
            font-weight: 700 !important;
        }
        div[class*="st-key-portal_hub_sign_out"] button:hover {
            background: #f8fafc !important; border-color: #94a3b8 !important;
        }
        /* Divider + space above Open buttons inside cards */
        div[class*="st-key-portal_select_od"],
        div[class*="st-key-portal_select_review_cycle"],
        div[class*="st-key-portal_select_sam"] {
            border-top: 1px solid #e2e8f0 !important;
            padding-top: 16px !important;
            margin-top: 8px !important;
        }
        div[class*="st-key-portal_select_od"] button {
            background: #2563eb !important; border-color: #2563eb !important; color: #fff !important;
            border-radius: 12px !important; font-weight: 650 !important; height: 48px !important;
            box-shadow: 0 8px 16px rgba(37, 99, 235, 0.22) !important;
        }
        div[class*="st-key-portal_select_review_cycle"] button {
            background: #0f766e !important; border-color: #0f766e !important; color: #fff !important;
            border-radius: 12px !important; font-weight: 650 !important; height: 48px !important;
            box-shadow: 0 8px 16px rgba(15, 118, 110, 0.22) !important;
        }
        div[class*="st-key-portal_select_sam"] button {
            background: #7c3aed !important; border-color: #7c3aed !important; color: #fff !important;
            border-radius: 12px !important; font-weight: 650 !important; height: 48px !important;
            box-shadow: 0 8px 16px rgba(124, 58, 237, 0.22) !important;
        }
        div[class*="st-key-portal_select_od"] button,
        div[class*="st-key-portal_select_review_cycle"] button,
        div[class*="st-key-portal_select_sam"] button {
            margin-top: 0 !important;
        }
        div[class*="st-key-portal_select_od"] button:hover,
        div[class*="st-key-portal_select_review_cycle"] button:hover,
        div[class*="st-key-portal_select_sam"] button:hover {
            filter: brightness(0.96); transform: translateY(-1px);
        }
        /* More air between the three portal cards */
        div[data-testid="stHorizontalBlock"]:has(.etc-hub-card) {
            align-items: stretch !important;
            gap: 1.5rem !important;
            column-gap: 1.5rem !important;
            margin-top: 4px !important;
        }
        div[data-testid="stHorizontalBlock"]:has(.etc-hub-card) [data-testid="stVerticalBlock"] {
            gap: 0.75rem !important;
        }
        div[data-testid="stHorizontalBlock"]:has(.etc-hub-card) > div[data-testid="stColumn"] {
            padding-left: 0.35rem !important;
            padding-right: 0.35rem !important;
        }
        @media (max-width: 900px) {
            [data-testid="stHorizontalBlock"]:has(.etc-hub-banner) { padding: 14px; }
            .main .block-container, .stMainBlockContainer, div[data-testid="stMainBlockContainer"] {
                padding: 1rem !important;
            }
            .etc-hub-banner-title { font-size: 24px; }
            div[data-testid="stHorizontalBlock"]:has(.etc-hub-card) {
                gap: 1rem !important;
            }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _portal_select_styles() -> None:
    st.markdown(
        """
        <style>
        .project-hero {
            background: linear-gradient(135deg, #0b6b95 0%, #1b8fb8 45%, #4fc3dc 100%);
            border-radius: 16px;
            padding: 22px 22px 18px 22px;
            margin: 4px 0 16px 0;
            color: #ffffff;
            box-shadow: 0 8px 24px rgba(12, 58, 86, 0.18);
        }
        .project-hero-title {
            margin: 0;
            font-size: 22px;
            font-weight: 800;
            letter-spacing: 0.2px;
        }
        .project-hero-sub {
            margin: 8px 0 0 0;
            font-size: 14px;
            color: #eaf7ff;
        }
        .project-select-wrap {
            background: linear-gradient(180deg, #f8fcff 0%, #eef6fb 100%);
            border: 1px solid #d8e8f5;
            border-radius: 14px;
            padding: 18px 20px;
            margin-bottom: 16px;
        }
        .project-select-title {
            margin: 0;
            font-size: 20px;
            font-weight: 700;
            color: #1d2e40;
        }
        .project-select-sub {
            margin: 6px 0 0 0;
            font-size: 14px;
            color: #3c4f63;
        }
        .project-card {
            border: 1px solid #d6e4ef;
            border-radius: 12px;
            padding: 14px 14px 12px 14px;
            background: #ffffff;
            margin: 6px 0 8px 0;
            min-height: 95px;
            box-shadow: 0 4px 12px rgba(19, 55, 79, 0.07);
        }
        .project-card-name {
            font-size: 16px;
            font-weight: 700;
            color: #1f2f43;
            margin: 0 0 4px 0;
            word-break: break-word;
        }
        .project-card-meta {
            font-size: 12px;
            color: #63758a;
            margin: 0;
        }
        .project-chip {
            display: inline-block;
            margin-top: 8px;
            font-size: 11px;
            font-weight: 700;
            color: #0d5b85;
            background: #e6f4ff;
            border: 1px solid #b9def8;
            border-radius: 999px;
            padding: 3px 9px;
        }
        .project-help {
            margin: 8px 0 14px 0;
            font-size: 13px;
            color: #4a5e73;
            text-align: center;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def enforce_client_project_session():
    """
    Block CLIENT users from hidden or unassigned projects (e.g. stale browser session).
    No-op for non-client roles. May call st.rerun() when redirecting.
    """
    user = st.session_state.get("user") or {}
    if str(user.get("role", "")).upper() != "CLIENT":
        return

    selected = st.session_state.get("selected_project")
    if not selected:
        return

    email = user.get("email", "")
    allowed = get_client_allowed_projects(email)
    visible = get_frontend_projects()

    session_ok = False
    if is_frontend_visible_project(selected):
        if allowed:
            session_ok = selected in allowed
        else:
            session_ok = selected in visible

    if session_ok:
        return

    st.session_state.pop("selected_project", None)
    st.session_state.pop("schema", None)

    if allowed:
        st.session_state["client_candidate_projects"] = allowed
        if len(allowed) == 1:
            st.session_state["selected_project"] = allowed[0]
            st.session_state["schema"] = visible[allowed[0]]
            st.rerun()
        st.query_params["page"] = "client_project_select"
        st.warning("Please choose an available project to continue.")
        st.rerun()

    st.session_state["logged_in"] = False
    st.session_state.pop("client_candidate_projects", None)
    st.query_params["page"] = "client_login"
    st.error(
        "Your assigned projects are no longer available in the client portal. "
        "Please contact your administrator."
    )
    st.rerun()


def client_project_select_page():
    """Post-login page for client users with multiple allowed projects."""
    user = st.session_state.get("user", {})
    role = str(user.get("role", "")).upper()
    if role != "CLIENT":
        st.error("This page is only for client users.")
        st.query_params["page"] = "login"
        return

    projects = st.session_state.get("client_candidate_projects") or []
    schema_value = get_frontend_projects()
    projects = [p for p in projects if p in schema_value]
    if not projects:
        st.error("No active projects found for your account. Contact administrator.")
        return

    if len(projects) == 1:
        chosen = projects[0]
        st.session_state["selected_project"] = chosen
        st.session_state["schema"] = schema_value[chosen]
        st.success(f"Project selected: {chosen}")
        st.query_params["page"] = "main"
        st.rerun()
        return

    _portal_select_styles()
    st.markdown(
        """
        <div class="project-hero">
            <p class="project-hero-title">Pick Your Dashboard</p>
            <p class="project-hero-sub">You have access to multiple projects. Choose one to continue.</p>
        </div>
        <div class="project-select-wrap">
            <p class="project-select-title">Choose Your Project</p>
            <p class="project-select-sub">Select one project to open your dashboard. You can switch later if you have multiple assigned projects.</p>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.markdown(
        "<p class='project-help'>Tip: project names match your assigned client programs.</p>",
        unsafe_allow_html=True,
    )

    cols = st.columns(2)
    for idx, project_name in enumerate(projects):
        col = cols[idx % 2]
        with col:
            st.markdown(
                f"""
                <div class="project-card">
                    <p class="project-card-name">{project_name}</p>
                    <p class="project-card-meta">Assigned client project</p>
                    <span class="project-chip">Available Now</span>
                </div>
                """,
                unsafe_allow_html=True,
            )
            if st.button(
                f"Enter {project_name}",
                key=f"client_project_card_{idx}_{project_name}",
                use_container_width=True,
                type="primary",
            ):
                st.session_state["selected_project"] = project_name
                st.session_state["schema"] = schema_value[project_name]
                st.session_state["client_candidate_projects"] = projects
                st.success(f"Project selected: {project_name}")
                st.query_params["page"] = "main"
                st.rerun()

def od_project_select_page():
    """Post-login project picker for staff OD Dashboard access."""
    user = st.session_state.get("user", {})
    role = str(user.get("role", "")).upper()
    if role == "CLIENT":
        st.error("Client users should use the client project picker.")
        st.query_params["page"] = "client_project_select"
        st.rerun()
        return

    schema_value = get_frontend_projects()
    projects = list(schema_value.keys())
    if not projects:
        st.error("No active projects found. Contact an administrator.")
        return

    if len(projects) == 1:
        chosen = projects[0]
        st.session_state["selected_project"] = chosen
        st.session_state["schema"] = schema_value[chosen]
        st.query_params["page"] = "main"
        st.rerun()
        return

    _portal_select_styles()
    st.markdown(
        """
        <div class="project-hero">
            <p class="project-hero-title">Pick Your Project</p>
            <p class="project-hero-sub">Choose an OD Collection project to open the dashboard.</p>
        </div>
        <div class="project-select-wrap">
            <p class="project-select-title">Choose Your Project</p>
            <p class="project-select-sub">You can switch projects later from the dashboard sidebar.</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    cols = st.columns(2)
    for idx, project_name in enumerate(projects):
        col = cols[idx % 2]
        with col:
            st.markdown(
                f"""
                <div class="project-card">
                    <p class="project-card-name">{project_name}</p>
                    <p class="project-card-meta">OD Collection Dashboard</p>
                    <span class="project-chip">Available</span>
                </div>
                """,
                unsafe_allow_html=True,
            )
            if st.button(
                f"Enter {project_name}",
                key=f"od_project_card_{idx}_{project_name}",
                use_container_width=True,
                type="primary",
            ):
                st.session_state["selected_project"] = project_name
                st.session_state["schema"] = schema_value[project_name]
                st.query_params["page"] = "main"
                st.rerun()

    st.divider()
    portals = allowed_portals(str(user.get("email", "")), role)
    if len(portals) > 1:
        if st.button("← Back to portal selection", use_container_width=True):
            st.query_params["page"] = "portal_select"
            st.rerun()


def portal_select_page():
    """Post-login portal picker (OD Dashboard, Review Cycle, Survey Assignment Manager)."""
    # Leaving RCD clears boot flag so the next RCD open uses a clean splash.
    st.session_state.pop("rcd_boot_complete", None)

    user = st.session_state.get("user", {})
    email = str(user.get("email", ""))
    role = str(user.get("role", "")).upper()
    username = _portal_display_name(user)
    role_label = _portal_role_label(email, role)
    portals = allowed_portals(email, role)

    if not portals:
        st.error("No portals are configured for your account. Contact an administrator.")
        return

    _portal_hub_styles()
    safe_name = html.escape(username)
    safe_role = html.escape(role_label)
    initials = html.escape(_portal_initials(username))

    # Top bar: brand + logged-in user (dynamic name + role)
    st.markdown(
        f"""
        <div class="etc-hub-topbar">
            <div class="etc-hub-brand">
                <span class="etc-hub-mark">ETC</span>
                <p class="etc-hub-brand-title">ETC OD Collection Platform</p>
            </div>
            <div class="etc-hub-userchip">
                <span class="etc-hub-avatar">{initials}</span>
                <div class="etc-hub-user-meta">
                    <p class="etc-hub-username">{safe_name}</p>
                    <p class="etc-hub-userrole">{safe_role}</p>
                </div>
                <span class="etc-hub-chevron" aria-hidden="true">▾</span>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # Welcome banner: icon + greeting on one row, compact Sign out on the right
    ban_left, ban_right = st.columns([6.2, 1], vertical_alignment="center")
    with ban_left:
        st.markdown(
            (
                '<div class="etc-hub-banner">'
                '<div class="etc-hub-banner-left">'
                '<div class="etc-hub-banner-icon" aria-hidden="true">'
                '<svg width="34" height="34" viewBox="0 0 34 34" fill="none">'
                '<rect x="2" y="6" width="16" height="16" rx="4" fill="#60a5fa" opacity="0.85"/>'
                '<rect x="10" y="2" width="16" height="16" rx="4" fill="#818cf8" opacity="0.75"/>'
                '<rect x="14" y="12" width="16" height="16" rx="4" fill="#38bdf8" opacity="0.7"/>'
                "</svg></div>"
                '<div class="etc-hub-banner-copy">'
                f'<span class="etc-hub-banner-title">Welcome back, {safe_name}!</span>'
                '<span class="etc-hub-banner-sub">Select a portal below to access your workspace.</span>'
                "</div></div></div>"
            ),
            unsafe_allow_html=True,
        )
    with ban_right:
        if st.button(
            "Sign out",
            key="portal_hub_sign_out",
            use_container_width=True,
            icon=":material/logout:",
        ):
            logout()

    st.markdown(
        (
            '<div class="etc-hub-section-head">'
            '<span class="etc-hub-section-icon" aria-hidden="true">'
            '<svg width="16" height="16" viewBox="0 0 24 24" fill="none">'
            '<rect x="3" y="3" width="8" height="8" rx="2" fill="currentColor"/>'
            '<rect x="13" y="3" width="8" height="8" rx="2" fill="currentColor" opacity="0.75"/>'
            '<rect x="3" y="13" width="8" height="8" rx="2" fill="currentColor" opacity="0.75"/>'
            '<rect x="13" y="13" width="8" height="8" rx="2" fill="currentColor" opacity="0.5"/>'
            "</svg></span>"
            '<div class="etc-hub-section-copy">'
            '<span class="etc-hub-section-title">Available Portals</span>'
            '<span class="etc-hub-section-sub">Choose the application you want to open.</span>'
            "</div></div>"
        ),
        unsafe_allow_html=True,
    )

    portal_cards: list[dict] = []
    if PORTAL_OD in portals:
        portal_cards.append(
            {
                "key": "od",
                "tone": "od",
                "check": "#2563eb",
                "title": "OD Collection Dashboard",
                "description": "Access collection, refusal analysis, and project reports.",
                "features": [
                    "Survey collection overview",
                    "Refusal analysis",
                    "Project reports & analytics",
                ],
                "button": "Open Dashboard →",
                "target_page": "od_project_select",
                "icon": (
                    '<svg width="22" height="22" viewBox="0 0 24 24" fill="none">'
                    '<path d="M4 19V10M10 19V5M16 19V13M22 19H2" '
                    'stroke="currentColor" stroke-width="2" stroke-linecap="round"/></svg>'
                ),
            }
        )
    if PORTAL_REVIEW_CYCLE in portals:
        portal_cards.append(
            {
                "key": "review_cycle",
                "tone": "rc",
                "check": "#0f766e",
                "title": "Review Cycle Dashboard",
                "description": "Review, cleaning, flags, and field workflows.",
                "features": [
                    "Data cleaning & review",
                    "Quality control & flags",
                    "Field workflow management",
                ],
                "button": "Open Review Dashboard →",
                "target_page": "review_cycle",
                "icon": (
                    '<svg width="22" height="22" viewBox="0 0 24 24" fill="none">'
                    '<rect x="6" y="3" width="12" height="18" rx="2" stroke="currentColor" stroke-width="2"/>'
                    '<path d="M9 9h6M9 13h6M9 17h3" stroke="currentColor" stroke-width="2" '
                    'stroke-linecap="round"/>'
                    '<circle cx="17" cy="17" r="4" fill="#ecfdf5" stroke="currentColor" stroke-width="1.5"/>'
                    '<path d="M15.5 17l1.2 1.2L18.8 16" stroke="currentColor" stroke-width="1.6" '
                    'stroke-linecap="round" stroke-linejoin="round"/></svg>'
                ),
            }
        )
    if PORTAL_SAM in portals and can_access_survey_assignment_manager(email, role):
        portal_cards.append(
            {
                "key": "sam",
                "tone": "sam",
                "check": "#7c3aed",
                "title": "Survey Assignment Manager",
                "description": "RunCut upload, assignment rules, Word report export.",
                "features": [
                    "RunCut upload & validation",
                    "Assignment rules management",
                    "Word report export",
                ],
                "button": "Open Assignment Manager →",
                "target_page": "field_assignments",
                "icon": (
                    '<svg width="22" height="22" viewBox="0 0 24 24" fill="none">'
                    '<circle cx="9" cy="8" r="3" stroke="currentColor" stroke-width="2"/>'
                    '<circle cx="16" cy="9" r="2.5" stroke="currentColor" stroke-width="2"/>'
                    '<path d="M4 18c0-2.5 2.2-4.5 5-4.5s5 2 5 4.5" stroke="currentColor" '
                    'stroke-width="2" stroke-linecap="round"/>'
                    '<path d="M14 18c0-1.6 1.2-3 3-3s3 1.4 3 3" stroke="currentColor" '
                    'stroke-width="2" stroke-linecap="round"/></svg>'
                ),
            }
        )

    if not portal_cards:
        st.warning("No portals are available for your account right now.")
        return

    cols = st.columns(min(len(portal_cards), 3) or 1, gap="large")
    for idx, card in enumerate(portal_cards):
        with cols[idx % len(cols)]:
            feature_items = "".join(
                _portal_feature_row(card["check"], point) for point in card["features"]
            )
            # No Current Project / Your Role blocks — features then CTA only
            # Compact HTML (no blank lines) so Streamlit markdown won't inject <p> and break flex rows
            st.markdown(
                (
                    f'<div class="etc-hub-card">'
                    f'<div class="etc-hub-card-top">'
                    f'<div class="etc-hub-card-icon {html.escape(card["tone"])}">{card["icon"]}</div>'
                    f'<div class="etc-hub-card-title">{html.escape(card["title"])}</div>'
                    f"</div>"
                    f'<p class="etc-hub-card-desc">{html.escape(card["description"])}</p>'
                    f'<div class="etc-hub-features">{feature_items}</div>'
                    f"</div>"
                ),
                unsafe_allow_html=True,
            )
            if st.button(
                card["button"],
                key=f"portal_select_{card['key']}",
                use_container_width=True,
                type="primary",
            ):
                st.query_params["page"] = card["target_page"]
                st.rerun()

    # Single Sign out stays in the welcome banner — footer is help + copyright only
    st.markdown(
        """
        <div class="etc-hub-footer">
            <div class="etc-hub-footer-left">
                <span>Need help? Contact your administrator.</span>
            </div>
            <div class="etc-hub-footer-right">
                <span>© 2026 ETC Institute</span>
                <span class="etc-hub-version">v1.0.0</span>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def admin_portal_select_page():
    """Backward-compatible alias for portal_select_page."""
    portal_select_page()

def is_authenticated():
    if "logged_in" in st.session_state and st.session_state.get("logged_in", False):
        token = st.session_state.get("token")
        if token:
            try:
                decoded_token = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
                return True
            except jwt.ExpiredSignatureError:
                st.warning("Session expired, please log in again.")
                logout()
            except jwt.InvalidTokenError:
                st.error("Invalid authentication token.")
                logout()
        else:
            return False
    else:
        return False

def decode_jwt(token):
    try:
        return jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
    except jwt.ExpiredSignatureError:
        st.warning("Session expired, please log in again.")
        logout()
    except jwt.InvalidTokenError:
        st.error("Invalid authentication token.")
        logout()

def generate_reset_token(email):
    payload = {
        "email": email,
        "exp": datetime.utcnow() + timedelta(minutes=15)
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)

def send_reset_email(user_email, reset_token):
    reset_link = app_public_page_link("reset_password", reset_token)
    subject = "Password Reset Request - ETC Institute"

    body = f"""
    <html>
    <head>
        <style>
            .container {{
                font-family: Arial, sans-serif;
                text-align: center;
                padding: 20px;
                background-color: #f4f4f4;
            }}
            .email-box {{
                background: white;
                padding: 30px;
                border-radius: 15px;
                box-shadow: 0 10px 30px rgba(0,0,0,0.1);
                max-width: 500px;
                margin: auto;
            }}
            .logo {{
                color: #667eea;
                font-size: 24px;
                font-weight: bold;
                margin-bottom: 20px;
            }}
            .btn {{
                display: inline-block;
                padding: 12px 30px;
                margin: 20px 0;
                font-size: 16px;
                color: #FFF;
                background: linear-gradient(135deg, #dc3545 0%, #c82333 100%);
                text-decoration: none;
                border-radius: 8px;
                font-weight: bold;
            }}
            .footer {{
                margin-top: 30px;
                font-size: 12px;
                color: #666;
                border-top: 1px solid #eee;
                padding-top: 20px;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="email-box">
                <div class="logo">ETC Institute</div>
                <h2>Password Reset Request</h2>
                <p>We received a request to reset your password. Click the button below to proceed:</p>
                <a class="btn" href="{reset_link}" style="color: #fff">Reset My Password</a>
                <p>If the button above doesn't work, you can also use this link:</p>
                <p><a href="{reset_link}">{reset_link}</a></p>
                <div class="footer">
                    <p>725 W. Frontier Lane, Olathe, KS | (913) 829-1215</p>
                    <p>Helping Organizations Make Better Decisions Since 1982</p>
                </div>
            </div>
        </div>
    </body>
    </html>
    """

    try:
        msg = MIMEMultipart()
        msg['From'] = EMAIL_ADDRESS
        msg['To'] = user_email
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'html'))

        with smtplib.SMTP(EMAIL_HOST, _smtp_port()) as server:
            server.starttls()
            server.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
            server.sendmail(EMAIL_ADDRESS, user_email, msg.as_string())
        return True
    except Exception as e:
        logger.exception("Failed to send password reset email to %s: %s", user_email, e)
        st.error(f"Could not send email. Check SMTP settings and logs. ({e})")
        return False

def forgot_password():
    """Displays the forgot password page."""
    def forgot_password_content():
        
        email = st.text_input("Email Address", placeholder="Enter your registered email")
        
        if st.button("Send Reset Link", type='primary', use_container_width=True):
            if not email:
                st.error("Email Field is Required")
                return
            
            conn = user_connect_to_snowflake()
            cursor = conn.cursor()
            cursor.execute("SELECT email FROM user.user_table WHERE email = %s", (email,))
            user = cursor.fetchone()

            if user:
                reset_token = generate_reset_token(email)
                if send_reset_email(email, reset_token):
                    st.success("Password reset link has been sent to your email!")
            else:
                st.error("Email not found in the system.")
            
            cursor.close()
            conn.close()
        
        st.markdown("""
            <style>
            .auth-link {
                text-align: center;
                margin-top: 1rem;
            }
            .auth-link a {
                color: rgb(29, 39, 96) !important;  /* brand color */
                text-decoration: none;
                font-weight: 600;
            }
            .auth-link a:hover {
                text-decoration: underline;
            }
            </style>
        """, unsafe_allow_html=True)

        st.markdown("""
            <div class="auth-link">
                Already have an account? <a href="/?page=login">Login here</a>
            </div>
        """, unsafe_allow_html=True)
    
    render_auth_layout(forgot_password_content, "Reset Password", "We'll help you get back into your account. Enter your email address and we'll send you a link to reset your password.")

def decode_reset_token(reset_token):
    try:
        decoded_payload = jwt.decode(reset_token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        return decoded_payload["email"]
    except jwt.ExpiredSignatureError:
        st.error("The reset link has expired.")
        return None
    except jwt.InvalidTokenError:
        st.error("Invalid reset token.")
        return None

def update_user_password(email, new_password):
    hashed_password = bcrypt.hashpw(new_password.encode('utf-8'), bcrypt.gensalt())
    encoded_hashed_password = base64.b64encode(hashed_password).decode('utf-8')

    conn = user_connect_to_snowflake()
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE user.user_table SET password = %s WHERE email = %s
    """, (encoded_hashed_password, email))
    conn.commit()
    cursor.close()
    conn.close()
    st.success("Password updated successfully!")

def reset_password():
    """Displays the reset password form."""
    def reset_password_content():
        reset_token = _query_param_first("token")

        if not reset_token:
            st.error("Reset token is missing or invalid.")
            return

        st.markdown("""
        <div style="text-align: center; margin-bottom: 30px;">
            <p>Create a new password for your account.</p>
        </div>
        """, unsafe_allow_html=True)
        
        new_password = st.text_input("New Password", type="password", placeholder="Enter new password")
        confirm_password = st.text_input("Confirm New Password", type="password", placeholder="Re-enter new password")
        
        if st.button("Reset Password", type='primary', use_container_width=True):
            if new_password != confirm_password:
                st.error("Passwords do not match.")
            else:
                email = decode_reset_token(reset_token)
                if email:
                    update_user_password(email, new_password)
                    st.success("Password reset successful! Redirecting to login...")
                    st.markdown(f'<meta http-equiv="refresh" content="2;url=/?page=login">', unsafe_allow_html=True)
                else:
                    st.error("Invalid reset token.")
        
        st.markdown("""
        <div class="auth-link">
            <a href="/?page=login">Back to Login</a>
        </div>
        """, unsafe_allow_html=True)
    
    render_auth_layout(reset_password_content, "Reset Password", "Create a new password for your account")

def generate_change_password_token(email):
    payload = {
        "email": email,
        "exp": datetime.utcnow() + timedelta(minutes=30)
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)

def send_change_password_email(email):
    token = generate_change_password_token(email)
    change_link = app_public_page_link("change_password", token)
    
    subject = "Change Your Password - ETC Institute"

    body = f"""
    <html>
    <head>
        <style>
            .container {{
                font-family: Arial, sans-serif;
                text-align: center;
                padding: 20px;
                background-color: #f4f4f4;
            }}
            .email-box {{
                background: white;
                padding: 30px;
                border-radius: 15px;
                box-shadow: 0 10px 30px rgba(0,0,0,0.1);
                max-width: 500px;
                margin: auto;
            }}
            .logo {{
                color: #667eea;
                font-size: 24px;
                font-weight: bold;
                margin-bottom: 20px;
            }}
            .btn {{
                display: inline-block;
                padding: 12px 30px;
                margin: 20px 0;
                font-size: 16px;
                color: #fff;
                background: linear-gradient(135deg, #007bff 0%, #0056b3 100%);
                text-decoration: none;
                border-radius: 8px;
                font-weight: bold;
            }}
            .footer {{
                margin-top: 30px;
                font-size: 12px;
                color: #666;
                border-top: 1px solid #eee;
                padding-top: 20px;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="email-box">
                <div class="logo">ETC Institute</div>
                <h2>Password Change Request</h2>
                <p>We received a request to change your password. Click the button below to proceed:</p>
                <a class="btn" href="{change_link}" style="color: #fff">Change My Password</a>
                <p>If the button above doesn't work, you can also use this link:</p>
                <p><a href="{change_link}">{change_link}</a></p>
                <div class="footer">
                    <p>This link is valid for <strong>30 minutes</strong>. If you did not request this, please ignore this email.</p>
                    <p>725 W. Frontier Lane, Olathe, KS | (913) 829-1215</p>
                    <p>Helping Organizations Make Better Decisions Since 1982</p>
                </div>
            </div>
        </div>
    </body>
    </html>
    """

    try:
        msg = MIMEMultipart()
        msg["From"] = EMAIL_ADDRESS
        msg["To"] = email
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "html"))

        with smtplib.SMTP(EMAIL_HOST, _smtp_port()) as server:
            server.starttls()
            server.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
            server.sendmail(EMAIL_ADDRESS, email, msg.as_string())

        st.success("Password change link sent to your email.")
    except Exception as e:
        logger.exception("Failed to send change-password email to %s: %s", email, e)
        st.error(f"Failed to send email: {e}")

def verify_change_password_token(token):
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=JWT_ALGORITHM)
        return payload["email"]
    except jwt.ExpiredSignatureError:
        st.error("Token has expired. Please request a new change password link.")
        return None
    except jwt.InvalidTokenError:
        st.error("Invalid token.")
        return None

def change_password_form():
    """Display the password change form after clicking the link."""
    def change_password_content():
        token = _query_param_first("token")

        if not token:
            st.error("Invalid request. No change password token found.")
            return

        email = verify_change_password_token(token)
        if not email:
            return

        st.markdown("""
        <div style="text-align: center; margin-bottom: 30px;">
            <p>Create a new password for your account.</p>
        </div>
        """, unsafe_allow_html=True)
        
        current_password = st.text_input("🔒 Current Password", type="password", placeholder="Enter your current password")
        new_password = st.text_input("🔒 New Password", type="password", placeholder="Enter your new password")
        confirm_password = st.text_input("🔒 Confirm New Password", type="password", placeholder="Re-enter your new password")
        
        if st.button("🔄 Change Password", use_container_width=True):
            if not current_password or not new_password or not confirm_password:
                st.error("All fields are required.")
                return
            
            if new_password != confirm_password:
                st.error("Passwords do not match.")
                return
            
            conn = user_connect_to_snowflake()
            cursor = conn.cursor()
            cursor.execute("SELECT password FROM user.user_table WHERE email = %s", (email,))
            user_record = cursor.fetchone()
            
            if not user_record:
                st.error("User not found.")
                cursor.close()
                conn.close()
                return
            
            stored_hashed_password = base64.b64decode(user_record[0])

            if not bcrypt.checkpw(current_password.encode('utf-8'), stored_hashed_password):
                st.error("Current password is incorrect.")
                cursor.close()
                conn.close()
                return

            hashed_password = bcrypt.hashpw(new_password.encode("utf-8"), bcrypt.gensalt())
            encoded_password = base64.b64encode(hashed_password).decode("utf-8")

            cursor.execute("UPDATE user.user_table SET password = %s WHERE email = %s", (encoded_password, email))
            conn.commit()
            cursor.close()
            conn.close()

            st.success("Password changed successfully. You can now log in.")
            st.markdown(f'<meta http-equiv="refresh" content="2;url=/?page=login">', unsafe_allow_html=True)
    
    render_auth_layout(change_password_content, "Change Password", "Update your account password")

def verify_user(email, current_password):
    try:
        conn = user_connect_to_snowflake()
        if not conn:
            logger.error("Failed to connect to Snowflake")
            return False
        
        cursor = conn.cursor()
        cursor.execute("SELECT password FROM user.user_table WHERE email = %s", (email,))
        user_record = cursor.fetchone()
        
        if not user_record:
            logger.debug(f"No user found with email: {email}")
            return False
            
        stored_hashed_password = base64.b64decode(user_record[0])
        is_valid = bcrypt.checkpw(current_password.encode('utf-8'), stored_hashed_password)
        logger.debug(f"Password verification result: {is_valid}")
        return is_valid
        
    except Exception as e:
        logger.error(f"Error in verify_user: {str(e)}")
        return False
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

def update_change_user_password(email, new_password):
    try:
        hashed_password = bcrypt.hashpw(new_password.encode('utf-8'), bcrypt.gensalt())
        encoded_hashed_password = base64.b64encode(hashed_password).decode('utf-8')

        conn = user_connect_to_snowflake()
        if not conn:
            logger.error("Database connection failed")
            return False
            
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE user.user_table 
            SET password = %s
            WHERE email = %s
        """, (encoded_hashed_password, email))
        
        rows_affected = cursor.rowcount
        conn.commit()
        
        logger.debug(f"Rows affected: {rows_affected}")
        return rows_affected > 0
        
    except Exception as e:
        logger.error(f"Error in update_change_user_password: {str(e)}")
        return False
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

def change_password(email):
    """Displays the change password form."""
    def change_password_content():
        if "form_submitted" not in st.session_state:
            st.session_state["form_submitted"] = False
            st.session_state["current_password"] = ""
            st.session_state["new_password"] = ""
            st.session_state["confirm_password"] = ""

        st.markdown("""
        <div style="text-align: center; margin-bottom: 30px;">
            <p>Update your password to keep your account secure.</p>
        </div>
        """, unsafe_allow_html=True)

        try:
            with st.form(key="change_password_form", clear_on_submit=False):
                current_password = st.text_input(
                    "🔒 Current Password",
                    type="password",
                    placeholder="Enter your current password",
                    key="current_pwd_input"
                )
                new_password = st.text_input(
                    "🔒 New Password",
                    type="password",
                    placeholder="Enter your new password",
                    key="new_pwd_input"
                )
                confirm_password = st.text_input(
                    "🔒 Confirm New Password",
                    type="password",
                    placeholder="Re-enter your new password",
                    key="confirm_pwd_input"
                )

                submit_button = st.form_submit_button("🔄 Update Password", use_container_width=True)

                if submit_button:
                    st.session_state["form_submitted"] = True
                    st.session_state["current_password"] = current_password
                    st.session_state["new_password"] = new_password
                    st.session_state["confirm_password"] = confirm_password

        except Exception as e:
            st.error(f"An unexpected error occurred: {str(e)}")
            logger.error(f"Unexpected error in change_password: {str(e)}")
            return

        if st.session_state["form_submitted"]:
            if not all([
                st.session_state["current_password"],
                st.session_state["new_password"],
                st.session_state["confirm_password"]
            ]):
                st.error("All fields are required")
                logger.debug("Validation failed: Empty fields")
                return

            if st.session_state["new_password"] != st.session_state["confirm_password"]:
                st.error("New passwords do not match")
                logger.debug("Validation failed: Passwords don't match")
                return

            logger.debug("Verifying user password")
            if not verify_user(email, st.session_state["current_password"]):
                st.error("Current password is incorrect")
                logger.debug("Verification failed: Incorrect current password")
                return

            logger.debug("Attempting to update password")
            success = update_change_user_password(email, st.session_state["new_password"])
            if success:
                st.success("Password updated successfully!")
                st.session_state["form_submitted"] = False
                st.session_state["current_password"] = ""
                st.session_state["new_password"] = ""
                st.session_state["confirm_password"] = ""
                logger.debug("Password update successful")
                
                st.markdown("""
                <div style="text-align: center; margin-top: 20px;">
                    <p>Your password has been updated successfully!</p>
                </div>
                """, unsafe_allow_html=True)
                
                if st.button("🏠 Return to Dashboard", use_container_width=True):
                    st.query_params["page"] = "main"
                    st.rerun()
            else:
                st.error("Failed to update password")
                logger.debug("Password update failed")

        if st.button("🔙 Back to Dashboard", use_container_width=True):
            for key in ["current_password", "new_password", "confirm_password", "form_submitted"]:
                if key in st.session_state:
                    del st.session_state[key]
            st.query_params["page"] = "main"
            st.rerun()
    
    render_auth_layout(change_password_content, "Change Password", "Update your account password")

# ============================================
# List of super admin emails (also used for refusal-blanks alert recipients)
SUPER_ADMIN_EMAILS = [
    "shehryar.iqbal@etcinstitute.com",
    "booali735@gmail.com",
    "jason.jones@etcinstitute.com",
]

REFUSAL_BLANKS_ALERT_THRESHOLD_PCT = 2.5


def get_refusal_blanks_alert_recipients():
    """All super admins receive refusal/blank alerts."""
    return list(SUPER_ADMIN_EMAILS)


def send_refusal_blanks_alert_email(to_emails, project_name, breaches, is_mock_test=False):
    """
    Send QA alert when configured fields exceed blank % threshold on a single day.
    breaches: list of dicts with keys field, date, pct, n, column_name (optional).
    """
    if not breaches or not to_emails:
        return False
    subject_prefix = "[TEST] " if is_mock_test else ""
    subject = f"{subject_prefix}Refusal/Blank Alert — {project_name}"
    rows_html = ""
    for b in breaches:
        rows_html += (
            f"<tr><td>{b.get('field', '')}</td>"
            f"<td>{b.get('date', '')}</td>"
            f"<td>{b.get('pct', '')}%</td>"
            f"<td>{b.get('n', '')}</td></tr>"
        )
    test_banner = ""
    if is_mock_test:
        test_banner = (
            '<div style="background:#fff3cd;border:1px solid #ffc107;padding:14px;margin-bottom:16px;">'
            "<strong>For testing the alert</strong> — this is a mock test email. "
            "No live survey data was changed. Threshold simulated: "
            f"&gt; {REFUSAL_BLANKS_ALERT_THRESHOLD_PCT}% blanks on a single day."
            "</div>"
        )
    body = f"""
    <html><body>
    {test_banner}
    <h2>Refusal / No Answer (Blanks) alert</h2>
    <p>Project: <strong>{project_name}</strong></p>
    <p>One or more Missing Alert fields exceeded {REFUSAL_BLANKS_ALERT_THRESHOLD_PCT}% blanks on a single day.</p>
    <table border="1" cellpadding="6" cellspacing="0">
    <tr><th>Field</th><th>Date</th><th>Blank %</th><th>N</th></tr>
    {rows_html}
    </table>
    <p>Open the dashboard → Refusal Analysis → Blanks % By Day tab for details.</p>
    </body></html>
    """
    try:
        msg = MIMEMultipart()
        msg["From"] = EMAIL_ADDRESS
        msg["To"] = ", ".join(to_emails)
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "html"))
        with smtplib.SMTP(EMAIL_HOST, _smtp_port()) as server:
            server.starttls()
            server.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
            server.sendmail(EMAIL_ADDRESS, to_emails, msg.as_string())
        return True
    except Exception as e:
        logger.exception("Failed to send refusal blanks alert: %s", e)
        return False


def evaluate_and_send_refusal_blanks_alerts(
    bucket_name,
    project_name,
    daily_df,
    alert_field_names,
    demographic_config,
):
    """
    Check daily blank % for alert fields; email all super admins for new breaches (>2.5%).
    Returns list of breach dicts emailed (may be empty).
    """
    from utils import load_refusal_blanks_alerts_from_s3, save_refusal_blanks_alerts_to_s3

    if not bucket_name or daily_df is None or daily_df.empty:
        return []
    if not alert_field_names:
        return []

    def _daily_n_by_date(df):
        """Map date label (e.g. '2026 03 18') -> total approaches N from bottom N row."""
        n_map = {}
        if "COLUMN_NAME" not in df.columns:
            return n_map
        n_rows = df[df["COLUMN_NAME"].astype(str).str.strip().str.upper() == "N"]
        if n_rows.empty:
            return n_map
        n_row = n_rows.iloc[-1]
        for col in df.columns:
            if not str(col).endswith("_BLANKS"):
                continue
            date_label = str(col).replace("_BLANKS", "")
            val = n_row.get(col, "")
            if val is None or str(val).strip() in ("", "nan", "None"):
                continue
            try:
                n_map[date_label] = int(round(float(val)))
            except (TypeError, ValueError):
                n_map[date_label] = val
        return n_map

    daily_n = _daily_n_by_date(daily_df)
    notified = load_refusal_blanks_alerts_from_s3(bucket_name, project_name)
    breaches = []
    threshold = REFUSAL_BLANKS_ALERT_THRESHOLD_PCT

    for _, row in daily_df.iterrows():
        if str(row.get("Alert", "")).strip() != "Alert":
            continue
        col_name = str(row.get("COLUMN_NAME", "")).strip()
        if col_name in ("N", "") or col_name.lower() == "n":
            continue
        for col in daily_df.columns:
            if not str(col).endswith("_PCT"):
                continue
            date_label = str(col).replace("_PCT", "")
            try:
                pct = float(row.get(col, 0) or 0)
            except (TypeError, ValueError):
                continue
            if pct <= threshold:
                continue
            key = f"{col_name}_{date_label}"
            if notified.get(key):
                continue
            breaches.append(
                {
                    "field": col_name,
                    "date": date_label,
                    "pct": round(pct, 2),
                    "n": daily_n.get(date_label, ""),
                }
            )
            notified[key] = True

    if not breaches:
        return []

    sent = send_refusal_blanks_alert_email(
        get_refusal_blanks_alert_recipients(),
        project_name,
        breaches,
    )
    if sent:
        save_refusal_blanks_alerts_to_s3(bucket_name, project_name, notified)
    return breaches if sent else []


def run_mock_refusal_blanks_alert_test(project_name):
    """
    Send a mock alert email to all super admins (no live data or S3 dedupe changes).
    Simulates Missing Alert fields breaching >2.5% on a single day.
    """
    mock_breaches = [
        {
            "field": "COUNT_VH_HH",
            "date": "2026 03 18",
            "pct": 8.75,
            "n": 320,
        },
        {
            "field": "TIME_ON",
            "date": "2026 03 18",
            "pct": 6.12,
            "n": 320,
        },
    ]
    recipients = get_refusal_blanks_alert_recipients()
    sent = send_refusal_blanks_alert_email(
        recipients,
        project_name,
        mock_breaches,
        is_mock_test=True,
    )
    return sent, recipients, mock_breaches


# SUPER ADMIN MANAGEMENT FUNCTIONS
# ============================================

def get_all_users():
    conn = user_connect_to_snowflake()
    cursor = conn.cursor(snowflake.connector.DictCursor)

    try:
        cursor.execute("""
            SELECT
                EMAIL,
                USERNAME,
                ROLE,
                IS_ACTIVE,
                CREATED_AT,
                LAST_LOGIN
            FROM USER.USER_TABLE
            ORDER BY EMAIL
        """)

        rows = cursor.fetchall()
        users = []

        for row in rows:
            users.append({
                "email": row["EMAIL"],
                "username": row["USERNAME"],
                "role": row["ROLE"],
                "is_active": row["IS_ACTIVE"],
                "created_at": (
                    row["CREATED_AT"].strftime("%Y-%m-%d %H:%M:%S")
                    if row["CREATED_AT"] else "N/A"
                ),
                "last_login": (
                    row["LAST_LOGIN"].strftime("%Y-%m-%d %H:%M:%S")
                    if row["LAST_LOGIN"] else "Never logged in"
                ),
            })

        return users

    except Exception as e:
        st.error(f"Error fetching users: {e}")
        return []

    finally:
        cursor.close()
        conn.close()

def toggle_user_status(email, new_status):
    """Activate or deactivate a user account."""
    conn = user_connect_to_snowflake()
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            UPDATE user.user_table 
            SET is_active = %s 
            WHERE email = %s
        """, (new_status, email))
        conn.commit()
        return cursor.rowcount > 0
    except Exception as e:
        st.error(f"Error updating user status: {e}")
        return False
    finally:
        cursor.close()
        conn.close()

def admin_create_user(email, username, password, role):
    """Create a new user account (admin function)."""
    conn = user_connect_to_snowflake()
    cursor = conn.cursor()
    
    try:
        # Check if user already exists
        cursor.execute("SELECT email FROM user.user_table WHERE email = %s", (email,))
        if cursor.fetchone():
            return False, "User already exists!"
        
        # Hash password
        hashed_password = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt())
        encoded_password = base64.b64encode(hashed_password).decode('utf-8')
        
        # Create user (active by default for admin-created accounts)
        cursor.execute("""
            INSERT INTO user.user_table (email, username, password, role, is_active, activation_token) 
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (email, username, encoded_password, role, True, None))
        
        conn.commit()
        return True, f"User {username} created successfully!"
    except Exception as e:
        return False, f"Error creating user: {str(e)}"
    finally:
        cursor.close()
        conn.close()

def admin_update_password(email, new_password):
    """Update a user's password (admin function)."""
    # Prevent updating passwords for super admins
    if is_super_admin(email):
        return False, "Cannot update password for super admin accounts."
    
    conn = user_connect_to_snowflake()
    cursor = conn.cursor()
    
    try:
        # Check if user exists
        cursor.execute("SELECT email FROM user.user_table WHERE email = %s", (email,))
        if not cursor.fetchone():
            return False, "User not found!"
        
        # Hash new password
        hashed_password = bcrypt.hashpw(new_password.encode('utf-8'), bcrypt.gensalt())
        encoded_password = base64.b64encode(hashed_password).decode('utf-8')
        
        # Update password
        cursor.execute("""
            UPDATE user.user_table 
            SET password = %s 
            WHERE email = %s
        """, (encoded_password, email))
        
        conn.commit()
        return True, f"Password updated successfully for {email}!"
    except Exception as e:
        return False, f"Error updating password: {str(e)}"
    finally:
        cursor.close()
        conn.close()


def admin_update_user_role(email: str, new_role: str) -> tuple[bool, str]:
    """Update a user's role (super-admin function). Super-admin accounts cannot be changed."""
    if is_super_admin(email):
        return False, "Cannot change role for super admin accounts."

    role = str(new_role or "").upper()
    if role not in {"USER", "ADMIN", "CLIENT", "CLEANING"}:
        return False, "Invalid role. Choose USER, ADMIN, CLIENT, or CLEANING."

    conn = user_connect_to_snowflake()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT email, role FROM user.user_table WHERE email = %s", (email,))
        row = cursor.fetchone()
        if not row:
            return False, "User not found!"
        if is_super_admin(row[0]):
            return False, "Cannot change role for super admin accounts."

        cursor.execute(
            """
            UPDATE user.user_table
            SET role = %s
            WHERE email = %s
            """,
            (role, email),
        )
        conn.commit()
        if cursor.rowcount <= 0:
            return False, "Role was not updated."
        return True, f"Role updated to {role} for {email}."
    except Exception as e:
        return False, f"Error updating role: {str(e)}"
    finally:
        cursor.close()
        conn.close()

# ============================================
# MANAGEMENT PAGE FUNCTIONS
# ============================================

def _user_initials(username: str) -> str:
    parts = [p for p in str(username or "").strip().split() if p]
    if not parts:
        return "?"
    if len(parts) == 1:
        return parts[0][:2].upper()
    return (parts[0][0] + parts[-1][0]).upper()


def _accounts_page_size_change() -> None:
    st.session_state["accounts_page_num"] = 1


def _accounts_management_styles() -> None:
    st.markdown(
        """
        <style>
        .acct-page-header h1 {
            margin: 0 0 4px 0;
            font-size: 28px;
            font-weight: 800;
            color: #1d2e40;
        }
        .acct-page-header p {
            margin: 0;
            color: #63758a;
            font-size: 14px;
        }
        .acct-stat-card {
            background: #ffffff;
            border: 1px solid #e6edf5;
            border-radius: 14px;
            padding: 16px 18px;
            box-shadow: 0 4px 14px rgba(19, 55, 79, 0.06);
            min-height: 92px;
        }
        .acct-stat-label {
            font-size: 13px;
            color: #63758a;
            margin-bottom: 8px;
        }
        .acct-stat-value {
            font-size: 30px;
            font-weight: 800;
            line-height: 1;
        }
        .acct-stat-total { color: #1565a8; }
        .acct-stat-active { color: #2e7d32; }
        .acct-stat-inactive { color: #c62828; }
        .acct-stats-gap {
            height: 22px;
        }
        .acct-actions-cell,
        .acct-btn-pw,
        .acct-btn-deact,
        .acct-btn-act,
        .acct-data-row {
            display: none !important;
        }
        div[data-testid="element-container"]:has(.acct-actions-cell),
        div[data-testid="element-container"]:has(.acct-btn-pw),
        div[data-testid="element-container"]:has(.acct-btn-deact),
        div[data-testid="element-container"]:has(.acct-btn-act),
        div[data-testid="element-container"]:has(.acct-data-row) {
            display: none !important;
            width: 0 !important;
            height: 0 !important;
            overflow: hidden !important;
            padding: 0 !important;
            margin: 0 !important;
            flex: 0 0 0 !important;
        }
        .acct-col-head {
            margin: 0;
            padding: 10px 0;
            font-size: 11px;
            font-weight: 700;
            color: #63758a;
            text-transform: uppercase;
            letter-spacing: 0.05em;
        }
        .acct-user-cell {
            display: flex;
            align-items: center;
            gap: 10px;
            min-height: 42px;
            max-width: 100%;
            overflow: hidden;
        }
        .acct-avatar {
            width: 38px;
            height: 38px;
            border-radius: 999px;
            background: linear-gradient(135deg, #0b6b95, #4fc3dc);
            color: #ffffff;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 13px;
            font-weight: 700;
            flex-shrink: 0;
        }
        .acct-user-name {
            font-size: 14px;
            font-weight: 700;
            color: #1f2f43;
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
            min-width: 0;
            flex: 1 1 auto;
            max-width: 100%;
        }
        .acct-email {
            font-size: 12px;
            color: #63758a;
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
            padding: 8px 4px 8px 0;
            line-height: 1.35;
            max-width: 100%;
            display: block;
        }
        .acct-role-badge {
            display: inline-block;
            padding: 6px 12px;
            border-radius: 999px;
            font-size: 11px;
            font-weight: 800;
            letter-spacing: 0.03em;
        }
        .acct-role-badge.super-admin {
            background: #f3e8ff;
            color: #7b1fa2;
            border: 1px solid #e1bee7;
        }
        .acct-role-locked {
            margin-top: 6px;
            font-size: 11px;
            color: #8a97a8;
        }
        .acct-status-pill {
            display: inline-flex;
            align-items: center;
            gap: 6px;
            padding: 6px 10px;
            border-radius: 999px;
            font-size: 12px;
            font-weight: 700;
            white-space: nowrap;
        }
        .acct-status-pill.active {
            background: #e8f5e9;
            color: #2e7d32;
        }
        .acct-status-pill.inactive {
            background: #ffebee;
            color: #c62828;
        }
        .acct-status-dot {
            width: 8px;
            height: 8px;
            border-radius: 999px;
            background: currentColor;
            flex-shrink: 0;
        }
        .acct-created {
            font-size: 12px;
            color: #63758a;
            padding: 12px 0;
            white-space: nowrap;
        }
        .acct-note {
            margin-top: 16px;
            background: #eef6ff;
            border: 1px solid #cfe3f7;
            border-radius: 12px;
            padding: 12px 14px;
            color: #35556f;
            font-size: 13px;
        }
        div[data-testid="stVerticalBlockBorderWrapper"]:has(.acct-table-anchor)
            div[data-testid="stHorizontalBlock"] {
            align-items: center !important;
        }
        div[data-testid="stVerticalBlockBorderWrapper"]:has(.acct-table-anchor)
            div[data-testid="stHorizontalBlock"] > div[data-testid="column"] {
            display: flex;
            align-items: center;
            min-height: 44px;
            overflow: hidden !important;
            min-width: 0 !important;
        }
        div[data-testid="stVerticalBlockBorderWrapper"]:has(.acct-table-anchor)
            div[data-testid="stHorizontalBlock"] > div[data-testid="column"] > div {
            width: 100% !important;
            min-width: 0 !important;
            overflow: hidden !important;
        }
        div[data-testid="stVerticalBlockBorderWrapper"]:has(.acct-table-anchor)
            div.acct-table-divider {
            height: 1px;
            background: #d8e0ea;
            margin: 0;
            width: 100%;
            display: block;
        }
        div[data-testid="stVerticalBlockBorderWrapper"]:has(.acct-table-anchor)
            div[data-testid="element-container"]:has(.acct-table-divider) {
            margin: 0 -1rem !important;
            padding: 0 1rem !important;
        }
        div[data-testid="stVerticalBlockBorderWrapper"]:has(.acct-table-anchor)
            div[data-testid="element-container"]:has(.acct-header-divider) {
            padding-top: 0 !important;
            padding-bottom: 8px !important;
        }
        div[data-testid="stVerticalBlockBorderWrapper"]:has(.acct-table-anchor)
            div[data-testid="element-container"]:has(.acct-user-row-divider) {
            padding-top: 10px !important;
            padding-bottom: 10px !important;
        }
        div[data-testid="stVerticalBlockBorderWrapper"]:has(.acct-table-anchor)
            div[data-testid="stSelectbox"] > div {
            margin-bottom: 0;
        }
        div[data-testid="stVerticalBlockBorderWrapper"]:has(.acct-table-anchor)
            div.acct-head-row + div[data-testid="stHorizontalBlock"] {
            background: #f8fbff;
            margin: -1rem -1rem 0 -1rem;
            padding: 0.2rem 1rem 0.05rem 1rem;
            min-height: 42px;
        }
        div[data-testid="stVerticalBlockBorderWrapper"]:has(.acct-table-anchor)
            div.acct-foot-row + div[data-testid="stHorizontalBlock"] {
            border-top: 1px solid #e6edf5;
            margin: 0 -1rem -1rem -1rem;
            padding: 8px 1rem 4px 1rem;
        }
        div[data-testid="column"]:has(.acct-actions-cell)
            div[data-testid="stVerticalBlock"] {
            display: block !important;
        }
        .acct-action-group {
            display: flex;
            flex-wrap: wrap;
            align-items: center;
            gap: 8px;
            max-width: 100%;
        }
        .acct-action-btn {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            gap: 5px;
            min-width: 68px;
            min-height: 30px;
            padding: 6px 10px;
            border-radius: 8px;
            color: #ffffff !important;
            font-size: 11px;
            font-weight: 700;
            line-height: 1;
            text-decoration: none !important;
            white-space: nowrap;
            box-shadow: 0 2px 6px rgba(0, 104, 148, 0.18);
            transition: transform 120ms ease, box-shadow 120ms ease, background 120ms ease;
        }
        .acct-action-btn:hover {
            color: #ffffff !important;
            text-decoration: none !important;
            transform: translateY(-1px);
            box-shadow: 0 4px 10px rgba(0, 104, 148, 0.22);
        }
        .acct-action-btn.password {
            background-color: #006894 !important;
            border: 1px solid #006894 !important;
        }
        .acct-action-btn.password:hover {
            background-color: #005477 !important;
        }
        .acct-action-btn.danger {
            background-color: #c62828 !important;
            border: 1px solid #c62828 !important;
        }
        .acct-action-btn.danger:hover {
            background-color: #ad1f1f !important;
        }
        .acct-action-btn.success {
            background-color: #2e7d32 !important;
            border: 1px solid #2e7d32 !important;
        }
        .acct-action-btn.success:hover {
            background-color: #246427 !important;
        }
        .acct-action-disabled {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            min-height: 30px;
            padding: 6px 10px;
            border-radius: 8px;
            background-color: #f0f4f8 !important;
            border: 1px solid #cfd8dc !important;
            color: #78909c !important;
            font-size: 11px;
            font-weight: 700;
            white-space: nowrap;
        }
        @media (max-width: 900px) {
            .acct-action-group {
                flex-direction: column;
                align-items: stretch;
            }
            .acct-action-btn,
            .acct-action-disabled {
                width: 100%;
            }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _accounts_role_change_handler(email: str, widget_key: str) -> None:
    new_role = st.session_state.get(widget_key)
    if not new_role:
        return
    success, message = admin_update_user_role(email, new_role)
    if success:
        st.session_state["accounts_role_toast"] = "Role updated successfully"
    else:
        st.session_state["accounts_role_error"] = message


def _filter_accounts_users(
    users: list[dict],
    search_term: str,
    role_filter: str,
    status_filter: str,
) -> list[dict]:
    filtered = users
    if search_term:
        term = search_term.lower()
        filtered = [
            u for u in filtered
            if term in u["username"].lower() or term in u["email"].lower()
        ]
    if role_filter != "All Roles":
        if role_filter == "Super Admin":
            filtered = [u for u in filtered if is_super_admin(u["email"])]
        else:
            filtered = [
                u for u in filtered
                if not is_super_admin(u["email"]) and str(u.get("role", "")).upper() == role_filter.upper()
            ]
    if status_filter == "Active":
        filtered = [u for u in filtered if u.get("is_active")]
    elif status_filter == "Inactive":
        filtered = [u for u in filtered if not u.get("is_active")]
    return filtered


def _accounts_users_csv(users: list[dict]) -> str:
    buffer = StringIO()
    writer = csv.writer(buffer)
    writer.writerow(["Username", "Email", "Role", "Status", "Created"])
    for user in users:
        role = "SUPER ADMIN" if is_super_admin(user["email"]) else str(user.get("role", "")).upper()
        writer.writerow([
            user.get("username", ""),
            user.get("email", ""),
            role,
            "Active" if user.get("is_active") else "Inactive",
            user.get("created_at", ""),
        ])
    return buffer.getvalue()


def _query_param_value(name: str) -> str:
    value = st.query_params.get(name, "")
    if isinstance(value, list):
        return value[0] if value else ""
    return str(value or "")


def _clear_accounts_action_params() -> None:
    for key in ("acct_action", "acct_email"):
        if key in st.query_params:
            del st.query_params[key]


def _accounts_action_url(email: str, action: str) -> str:
    return "?" + urlencode({
        "page": "accounts_management",
        "acct_action": action,
        "acct_email": email,
    })


def _accounts_action_link(email: str, action: str, label: str, css_class: str, title: str) -> str:
    return (
        f'<a class="acct-action-btn {css_class}" '
        f'href="{html.escape(_accounts_action_url(email, action), quote=True)}" '
        f'title="{html.escape(title, quote=True)}">{html.escape(label)}</a>'
    )


def _handle_accounts_action_query() -> None:
    action = _query_param_value("acct_action")
    email = _query_param_value("acct_email")
    if not action or not email:
        return

    if is_super_admin(email):
        st.session_state["accounts_role_error"] = "Protected account cannot be changed."
        _clear_accounts_action_params()
        st.rerun()

    if action == "password":
        st.session_state["password_update_target_email"] = email
        st.session_state.pop("password_update_selected_email", None)
        _clear_accounts_action_params()
        st.query_params["page"] = "password_update"
        st.rerun()

    if action in {"activate", "deactivate"}:
        make_active = action == "activate"
        if toggle_user_status(email, make_active):
            status_label = "activated" if make_active else "deactivated"
            st.session_state["accounts_role_toast"] = f"{email} {status_label} successfully."
        _clear_accounts_action_params()
        st.query_params["page"] = "accounts_management"
        st.rerun()


def accounts_management_page():
    """Display all accounts with activate/deactivate functionality."""
    current_user_email = st.session_state.get("user", {}).get("email", "")
    if not is_super_admin(current_user_email):
        st.error("Access Denied: This page is only accessible to super administrators.")
        return

    _accounts_management_styles()
    _handle_accounts_action_query()

    header_left, header_right = st.columns([3.2, 1])
    with header_left:
        st.markdown(
            """
            <div class="acct-page-header">
                <h1>Accounts Management</h1>
                <p>Manage user accounts, roles and permissions</p>
            </div>
            """,
            unsafe_allow_html=True,
        )
    with header_right:
        st.markdown("<div style='height: 18px'></div>", unsafe_allow_html=True)
        if st.button("Create Account", type="primary", use_container_width=True):
            st.query_params["page"] = "create_accounts"
            st.rerun()

    users = get_all_users()
    if not users:
        st.info("No users found in the system.")
        return

    total_users = len(users)
    active_users = sum(1 for u in users if u["is_active"])
    inactive_users = total_users - active_users

    stat1, stat2, stat3 = st.columns(3)
    with stat1:
        st.markdown(
            f"""
            <div class="acct-stat-card">
                <div class="acct-stat-label">Total Users</div>
                <div class="acct-stat-value acct-stat-total">{total_users}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    with stat2:
        st.markdown(
            f"""
            <div class="acct-stat-card">
                <div class="acct-stat-label">Active Users</div>
                <div class="acct-stat-value acct-stat-active">{active_users}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    with stat3:
        st.markdown(
            f"""
            <div class="acct-stat-card">
                <div class="acct-stat-label">Inactive Users</div>
                <div class="acct-stat-value acct-stat-inactive">{inactive_users}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    st.markdown('<div class="acct-stats-gap"></div>', unsafe_allow_html=True)

    if toast := st.session_state.pop("accounts_role_toast", None):
        st.success(toast)
    if err := st.session_state.pop("accounts_role_error", None):
        st.error(err)

    if "accounts_page_num" not in st.session_state:
        st.session_state["accounts_page_num"] = 1

    with st.container(border=True):
        tool1, tool2, tool3, tool4 = st.columns([2.4, 1, 1, 1], gap="small")
        with tool1:
            search_term = st.text_input(
                "Search",
                placeholder="Search by username or email...",
                label_visibility="collapsed",
                key="accounts_search",
            )
        with tool2:
            role_filter = st.selectbox(
                "Role filter",
                ["All Roles", "USER", "ADMIN", "CLIENT", "Super Admin"],
                label_visibility="collapsed",
                key="accounts_role_filter",
            )
        with tool3:
            status_filter = st.selectbox(
                "Status filter",
                ["All Statuses", "Active", "Inactive"],
                label_visibility="collapsed",
                key="accounts_status_filter",
            )
        with tool4:
            filtered_for_export = _filter_accounts_users(users, search_term, role_filter, status_filter)
            st.download_button(
                "Export CSV",
                data=_accounts_users_csv(filtered_for_export),
                file_name="accounts_export.csv",
                mime="text/csv",
                type="primary",
                use_container_width=True,
            )

    filtered_users = filtered_for_export
    if not filtered_users:
        st.info("No users found matching your search criteria.")
        return

    filter_sig = f"{search_term}|{role_filter}|{status_filter}"
    if st.session_state.get("accounts_filter_sig") != filter_sig:
        st.session_state["accounts_filter_sig"] = filter_sig
        st.session_state["accounts_page_num"] = 1

    page_size = int(st.session_state.get("accounts_page_size", 10))
    if page_size not in {5, 10, 25, 50}:
        page_size = 10
    total_pages = max(1, (len(filtered_users) + page_size - 1) // page_size)
    page_num = st.session_state.get("accounts_page_num", 1)
    page_num = max(1, min(page_num, total_pages))
    st.session_state["accounts_page_num"] = page_num
    start = (page_num - 1) * page_size
    page_users = filtered_users[start:start + page_size]
    col_ratios = [1.45, 2.0, 1.0, 0.85, 1.0, 1.8]
    role_options = ["USER", "ADMIN", "CLIENT", "CLEANING"]

    with st.container(border=True):
        st.markdown('<div class="acct-table-anchor"></div>', unsafe_allow_html=True)
        st.markdown('<div class="acct-head-row"></div>', unsafe_allow_html=True)
        head_cols = st.columns(col_ratios, gap="small")
        for col, label in zip(head_cols, ["User", "Email", "Role", "Status", "Created", "Actions"]):
            with col:
                st.markdown(f'<p class="acct-col-head">{label}</p>', unsafe_allow_html=True)

        st.markdown(
            '<div class="acct-table-divider acct-header-divider"></div>',
            unsafe_allow_html=True,
        )

        for idx, user in enumerate(page_users):
            st.markdown('<div class="acct-data-row"></div>', unsafe_allow_html=True)

            user_email = user["email"]
            username = user.get("username", "")
            display_username = username.split("@", 1)[0] if "@" in str(username) else username
            is_super = is_super_admin(user_email)
            initials = _user_initials(display_username)
            created_display = user.get("created_at", "N/A") or "N/A"
            status_class = "active" if user.get("is_active") else "inactive"
            status_label = "Active" if user.get("is_active") else "Inactive"

            row1, row2, row3, row4, row5, row6 = st.columns(col_ratios, gap="small")
            with row1:
                st.markdown(
                    f"""
                    <div class="acct-user-cell">
                        <div class="acct-avatar">{html.escape(initials)}</div>
                        <div class="acct-user-name" title="{html.escape(username)}">{html.escape(display_username)}</div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
            with row2:
                st.markdown(
                    f'<div class="acct-email">{html.escape(user_email)}</div>',
                    unsafe_allow_html=True,
                )
            with row3:
                if is_super:
                    st.markdown(
                        """
                        <span class="acct-role-badge super-admin">SUPER ADMIN</span>
                        <div class="acct-role-locked">Role cannot be changed</div>
                        """,
                        unsafe_allow_html=True,
                    )
                else:
                    current_role = str(user.get("role", "USER")).upper()
                    if current_role not in role_options:
                        current_role = "USER"
                    role_key = f"role_select_{user_email}"
                    st.selectbox(
                        "Role",
                        role_options,
                        index=role_options.index(current_role),
                        key=role_key,
                        label_visibility="collapsed",
                        on_change=_accounts_role_change_handler,
                        args=(user_email, role_key),
                    )
            with row4:
                st.markdown(
                    f"""
                    <span class="acct-status-pill {status_class}">
                        <span class="acct-status-dot"></span>{status_label}
                    </span>
                    """,
                    unsafe_allow_html=True,
                )
            with row5:
                st.markdown(
                    f'<div class="acct-created">{html.escape(created_display)}</div>',
                    unsafe_allow_html=True,
                )
            with row6:
                st.markdown('<div class="acct-actions-cell"></div>', unsafe_allow_html=True)
                if is_super:
                    st.markdown(
                        '<div class="acct-action-group">'
                        '<span class="acct-action-disabled" title="Protected account">Protected</span>'
                        '</div>',
                        unsafe_allow_html=True,
                    )
                elif user.get("is_active"):
                    st.markdown(
                        '<div class="acct-action-group">'
                        + _accounts_action_link(user_email, "password", "Pass", "password", "Change password")
                        + _accounts_action_link(user_email, "deactivate", "Deact", "danger", "Deactivate account")
                        + '</div>',
                        unsafe_allow_html=True,
                    )
                else:
                    st.markdown(
                        '<div class="acct-action-group">'
                        + _accounts_action_link(user_email, "password", "Pass", "password", "Change password")
                        + _accounts_action_link(user_email, "activate", "Active", "success", "Activate account")
                        + '</div>',
                        unsafe_allow_html=True,
                    )

            if idx < len(page_users) - 1:
                st.markdown(
                    '<div class="acct-table-divider acct-user-row-divider"></div>',
                    unsafe_allow_html=True,
                )

        st.markdown('<div class="acct-foot-row"></div>', unsafe_allow_html=True)
        foot_left, foot_mid, foot_right = st.columns([1.5, 1.2, 1], gap="small")
        with foot_left:
            st.caption(
                f"Showing {start + 1} to {min(start + len(page_users), len(filtered_users))} "
                f"of {len(filtered_users)} users"
            )
        with foot_mid:
            pcol1, pcol2, pcol3 = st.columns(3, gap="small")
            with pcol1:
                if st.button("Prev", disabled=page_num <= 1, key="accounts_prev_page"):
                    st.session_state["accounts_page_num"] = page_num - 1
                    st.rerun()
            with pcol2:
                st.markdown(
                    f"<div style='text-align:center;padding-top:6px;color:#63758a;'>"
                    f"Page {page_num} / {total_pages}</div>",
                    unsafe_allow_html=True,
                )
            with pcol3:
                if st.button("Next", disabled=page_num >= total_pages, key="accounts_next_page"):
                    st.session_state["accounts_page_num"] = page_num + 1
                    st.rerun()
        with foot_right:
            st.selectbox(
                "Rows per page",
                [10, 5, 25, 50],
                label_visibility="collapsed",
                key="accounts_page_size",
                on_change=_accounts_page_size_change,
            )

    st.markdown(
        """
        <div class="acct-note">
            <strong>Note:</strong> Role changes take effect immediately. Users may need to log in again to see updated permissions.
        </div>
        """,
        unsafe_allow_html=True,
    )

def create_accounts_page():
    """Page for creating new user accounts."""
    st.title("Create User Account")
    st.markdown("**Create new admin or client accounts**")
    
    # Get current user email
    current_user_email = st.session_state.get("user", {}).get("email", "")
    
    if not is_super_admin(current_user_email):
        st.error("❌ Access Denied: This page is only accessible to super administrators.")
        return
    
    with st.form("create_account_form"):
        col1, col2 = st.columns(2)
        
        with col1:
            username = st.text_input("Username *", placeholder="Enter username")
            email = st.text_input("Email *", placeholder="Enter email address")
        
        with col2:
            password = st.text_input("Password *", type="password", placeholder="Enter password")
            confirm_password = st.text_input("Confirm Password *", type="password", placeholder="Re-enter password")
        
        role = st.selectbox("Role *", ["ADMIN", "CLIENT", "CLEANING"], help="Select user role")
        
        st.markdown("**Note:** Accounts created by super admins are automatically activated.")
        
        if st.form_submit_button("Create Account", type="primary", use_container_width=True):
            if not all([username, email, password, confirm_password]):
                st.error("Please fill in all required fields.")
            elif password != confirm_password:
                st.error("Passwords do not match.")
            else:
                success, message = admin_create_user(email, username, password, role)
                if success:
                    st.success(message)
                    st.balloons()
                    time.sleep(1)
                    st.rerun()
                else:
                    st.error(message)

    # Add a button to go back to Accounts Management
    if st.button("← Back to Accounts Management", type="secondary"):
        # Clear the selected email when going back
        if "password_update_selected_email" in st.session_state:
            del st.session_state["password_update_selected_email"]
        st.query_params["page"] = "accounts_management"
        st.rerun()

def password_update_page():
    """Page for updating user passwords."""
    st.title("Password Update")
    st.markdown("**Update passwords for any user account**")
    
    current_user_email = st.session_state.get("user", {}).get("email", "")
    if not is_super_admin(current_user_email):
        st.error("❌ Access Denied: This page is only accessible to super administrators.")
        return
    
    all_users = get_all_users()
    non_admin_users = [u for u in all_users if not is_super_admin(u['email'])]
    if not non_admin_users:
        st.info("No users available for password update.")
        return
    
    target_email = st.session_state.pop("password_update_target_email", None)
    if target_email:
        st.session_state["password_update_selected_email"] = target_email
    
    selected_email = st.session_state.get("password_update_selected_email")
    if not selected_email:
        st.warning("⚠️ **Note:** You cannot update passwords for super admin accounts (Jason, Shehryar, Boo Ali).")
        user_options = {f"{u['username']} ({u['email']})": u['email'] for u in non_admin_users}
        selected_user_display = st.selectbox("Select User", list(user_options.keys()), key="password_update_user_picker")
        selected_email = user_options[selected_user_display]
        st.session_state["password_update_selected_email"] = selected_email
    
    selected_user = next((u for u in non_admin_users if u["email"] == selected_email), None)
    if not selected_user:
        st.error(f"User with email {selected_email} not found or is a super admin.")
        st.session_state.pop("password_update_selected_email", None)
        return
    
    if is_super_admin(selected_user["email"]):
        st.error("⚠️ **Cannot update password for super admin accounts.**")
        st.session_state.pop("password_update_selected_email", None)
        return
    
    email_to_update = selected_user["email"]
    st.info(
        f"**Updating password for:** {selected_user['username']} ({email_to_update}) | "
        f"**Role:** {selected_user['role']}"
    )
    
    with st.form(f"update_password_form_{email_to_update}"):
        st.text_input("Account email (locked)", value=email_to_update, disabled=True)
        new_password = st.text_input("New Password *", type="password", placeholder="Enter new password")
        confirm_password = st.text_input("Confirm New Password *", type="password", placeholder="Re-enter new password")
        
        if st.form_submit_button("Update Password", type="primary", use_container_width=True):
            if not new_password or not confirm_password:
                st.error("Please fill in both password fields.")
            elif new_password != confirm_password:
                st.error("Passwords do not match.")
            elif len(new_password) < 6:
                st.error("Password must be at least 6 characters long.")
            else:
                success, message = admin_update_password(email_to_update, new_password)
                if success:
                    st.success(message)
                    st.balloons()
                    time.sleep(1)
                    st.rerun()
                else:
                    st.error(message)
    
    if st.button("← Back to Accounts Management", type="secondary", key="password_update_back"):
        st.session_state.pop("password_update_selected_email", None)
        st.query_params["page"] = "accounts_management"
        st.rerun()