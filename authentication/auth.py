import os
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

# Set up basic logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
load_dotenv()

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
schema_value = {'SALEM': 'salem_bus', 'ACTRANSIT': 'actransit_bus', 'KCATA': 'kcata_bus', 'KCATA RAIL': 'kcata_rail'}

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


def render_auth_layout(content_function, title, subtitle=None, dashboard_type="supervisor"):
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
                    <h4 style="margin-bottom:10px">TRANSIT SURVEY 2025</h4>
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
    activation_link = f"http://18.116.237.208:8501/?page=activate&token={activation_token}"
    
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

        with smtplib.SMTP(EMAIL_HOST, EMAIL_PORT) as server:
            server.starttls()
            server.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
            server.sendmail(EMAIL_ADDRESS, email, msg.as_string())

        print("Activation email sent successfully!")
    except Exception as e:
        print(f"Failed to send email: {e}")

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
        send_activation_email(email, activation_token)
        st.success(f"User {username} created successfully! Activation email sent.")
        return True
    except Exception as e:
        st.error(f"Failed to create user: {e}")
        return False
    finally:
        cursor.close()
        conn.close()

def create_new_user_page():
    """Admin panel page to create new users."""
    def create_user_content():
        with st.form(key="create_new_user_form"):
            # First row - Username and Email
            col1, col2 = st.columns(2)
            with col1:
                username = st.text_input("Username", placeholder="Enter username")
            with col2:
                email = st.text_input("Email", placeholder="Enter user email")
            
            # Second row - Role and Password
            col3, col4 = st.columns(2)
            with col3:
                password = st.text_input("Password", type="password", placeholder="Enter password")
            with col4:
                confirm_password = st.text_input("Confirm Password", type="password", placeholder="Re-enter password")
            
            # Third row - Confirm Password (full width)
            role = st.selectbox("Role", ["USER", "ADMIN"])

            if st.form_submit_button("Create User", type="primary", use_container_width=True):
                if not all([username, email, password, confirm_password]):
                    st.error("Please complete all fields.")
                elif password != confirm_password:
                    st.error("Passwords do not match.")
                else:
                    try:
                        if create_new_user(email, username, password, role):
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

    render_auth_layout(create_user_content, "Create User", "")

def register_new_user(email, username, password, role):
    conn = user_connect_to_snowflake()
    cursor = conn.cursor()

    cursor.execute("SELECT email FROM user.user_table WHERE email = %s", (email,))
    if cursor.fetchone():
        st.error('User already exists!')
        return False

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

    send_activation_email(email, activation_token)
    return True

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
                    if register_new_user(email, username, password1, 'CLIENT'):
                        st.success("Registration successful! Check your email for activation link.")
                        time.sleep(2)
                        st.markdown(f'<meta http-equiv="refresh" content="0;url=/?page=login">', unsafe_allow_html=True)

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


def activate_account():
    """Activate the user account based on the verification token."""
    def activate_content():
        query_params = st.query_params
        token = query_params.get("token", None)

        if not token:
            st.error("Invalid activation link.")
            return

        conn = user_connect_to_snowflake()
        cursor = conn.cursor()
        
        cursor.execute("SELECT email FROM user.user_table WHERE activation_token = %s", (token,))
        user_record = cursor.fetchone()

        if not user_record:
            st.error("Invalid or expired activation token.")
            cursor.close()
            conn.close()
            return

        email = user_record[0]

        cursor.execute("UPDATE user.user_table SET is_active = %s, activation_token = NULL WHERE email = %s", (True, email))
        conn.commit()
        cursor.close()
        conn.close()

        st.success("🎉 Your account has been activated! You can now log in.")
        st.info("Redirecting to login page...")
        st.markdown(f'<meta http-equiv="refresh" content="2;url=/?page=login">', unsafe_allow_html=True)
    
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

def login():
    """Displays a login form and handles authentication."""
    def login_content():
        with st.form(key="login_form"):
            # Email and Password labels will be black if theme textColor is set to black
            email = st.text_input("Email", placeholder="Enter your email address")
            password = st.text_input("Password", type="password", placeholder="Enter your password")

            project = st.selectbox("Select a Project", list(schema_value.keys()))
            # Forgot Password link, right-aligned and red
            st.markdown(
                '<div style="text-align: right; margin-bottom: 8px;">'
                '<a href="/?page=forgot_password" style="color: red; text-decoration: underline;">Forgot Password?</a>'
                '</div>',
                unsafe_allow_html=True
            )
            # Login button
            if st.form_submit_button("Login", type="primary", use_container_width=True):
                user = check_user_login(email, password)
                if user == "inactive":
                    st.error("Your account is not active. Please verify your email before logging in.")
                elif user:
                    store_user_in_session(user)
                    st.success(f"Welcome {user['username']}!")
                    jwt_token = generate_jwt(user["email"], user["username"], user["role"])
                    st.session_state["logged_in"] = True
                    st.session_state["jwt_token"] = jwt_token
                    st.session_state["selected_project"] = project
                    st.session_state["schema"] = schema_value[project]
                    st.query_params["logged_in"] = "true"
                    st.query_params["page"] = "main"
                    st.rerun()
                else:
                    st.error("Incorrect email or password")

        # "Do not have an account? Create Account" link, right-aligned
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
                    No account yet? Reach out to admin to get started!
                </p>
            </div>
            ''',
            unsafe_allow_html=True
        )

    render_auth_layout(login_content, "Login", "")

def logout():
    st.session_state.clear()
    st.success("Logged out successfully!")
    st.query_params["page"] = "login"
    st.rerun()

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
    reset_link = f"http://18.116.237.208:8501/?page=reset_password&token={reset_token}"
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

        with smtplib.SMTP(EMAIL_HOST, EMAIL_PORT) as server:
            server.starttls()
            server.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
            server.sendmail(EMAIL_ADDRESS, user_email, msg.as_string())
        st.success("Password reset link sent to your email.")
    except Exception as e:
        print(f"Failed to send email: {e}")

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
                send_reset_email(email, reset_token)
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
        query_params = st.query_params
        reset_token = query_params.get("token", None)

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
    change_link = f"http://18.116.237.208:8501/?page=change_password&token={token}"
    
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

        with smtplib.SMTP(EMAIL_HOST, EMAIL_PORT) as server:
            server.starttls()
            server.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
            server.sendmail(EMAIL_ADDRESS, email, msg.as_string())

        st.success("Password change link sent to your email.")
    except Exception as e:
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
        query_params = st.query_params
        token = query_params.get("token", None)

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