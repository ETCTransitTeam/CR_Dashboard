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


schema_value = {'TUCSON': 'tucson_bus','TUCSON RAIL': 'tucson_rail','VTA': 'public', 'UTA': 'uta_rail', 'STL':'stl_bus', 'KCATA': 'kcata_bus'}


def send_activation_email(email, activation_token):
    """Send an account activation email with a secure token using HTML format."""
    activation_link = f"http://18.116.237.208:8501/?page=activate&token={activation_token}"
    
    subject = "Activate Your Account"
    
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
                padding: 20px;
                border-radius: 8px;
                box-shadow: 0px 0px 10px rgba(0, 0, 0, 0.1);
                max-width: 500px;
                margin: auto;
            }}
            .btn {{
                display: inline-block;
                padding: 10px 20px;
                margin-top: 20px;
                font-size: 16px;
                color: #FFF;
                background-color: #007BFF;
                text-decoration: none;
                border-radius: 5px;
            }}
            .btn:hover {{
                background-color: #0056b3;
            }}
            .footer {{
                margin-top: 20px;
                font-size: 12px;
                color: #666;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="email-box">
                <h2>Welcome to Our Service!</h2>
                <p>Thank you for signing up. Please activate your account by clicking the button below:</p>
                <a class="btn" href="{activation_link}" style="color: #fff">Activate My Account</a>
                <p>If the button above doesn't work, you can also use this link:</p>
                <p><a href="{activation_link}">{activation_link}</a></p>
                <p class="footer">If you did not sign up for this account, please ignore this email.</p>
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
        msg.attach(MIMEText(body, 'html'))  # Use 'html' instead of 'plain'

        with smtplib.SMTP(EMAIL_HOST, EMAIL_PORT) as server:
            server.starttls()
            server.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
            server.sendmail(EMAIL_ADDRESS, email, msg.as_string())

        print("Activation email sent successfully!")  # Replace with `st.success()` if using Streamlit
    except Exception as e:
        print(f"Failed to send email: {e}")  # Replace with `st.error()` if using Streamlit


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
    """Displays a form for admins to create new users."""
    st.title("Create New User")

    # Initialize minimal session state for navigation and messages
    if "current_page" not in st.session_state:
        st.session_state["current_page"] = "create_user"
    if "error_message" not in st.session_state:
        st.session_state["error_message"] = ""
    if "success_message" not in st.session_state:
        st.session_state["success_message"] = ""

    # Display error message if it exists
    if st.session_state["error_message"]:
        st.error(st.session_state["error_message"])

    # Display success message if it exists
    if st.session_state["success_message"]:
        st.success(st.session_state["success_message"])
        # Refresh the page after 2 seconds to clear the form
        time.sleep(2)  # Delay to show the message
        st.markdown(f'<meta http-equiv="refresh" content="0;url=/?page=create_user">', unsafe_allow_html=True)
        st.stop()  # Stop execution to prevent further rendering until refresh

    with st.form(key="create_new_user_form"):
        # Form inputs without session state persistence
        username = st.text_input("Username")
        email = st.text_input("Email")
        role = st.selectbox("Select Role", ["USER", "ADMIN"], index=0)  # Default to USER
        password = st.text_input("Password", type="password")
        confirm_password = st.text_input("Confirm Password", type="password")
        
        submit_button = st.form_submit_button(label="ADD User")

        if submit_button:
            st.session_state["error_message"] = ""
            st.session_state["success_message"] = ""

            if not all([username, email, password, confirm_password]):
                st.session_state["error_message"] = "Please fill in all fields."
            elif password != confirm_password:
                st.session_state["error_message"] = "Passwords do not match."
            else:
                try:
                    if create_new_user(email, username, password, role):
                        st.session_state["success_message"] = "User created successfully! Activation email sent."
                        time.sleep(5)  # Delay to show the message

                        st.markdown(f'<meta http-equiv="refresh" content="0;url=/?page=create_user">', unsafe_allow_html=True)

                        # The success message will trigger the refresh above
                    else:
                        st.session_state["error_message"] = "Failed to create user."
                except Exception as e:
                    st.session_state["error_message"] = f"Error: {str(e)}"

    # Navigation button
    if st.button("Login"):
        st.markdown(f'<meta http-equiv="refresh" content="0;url=/?page=login">', unsafe_allow_html=True)
        

def register_new_user(email, username, password,role):
    conn = user_connect_to_snowflake()
    cursor = conn.cursor()

    cursor.execute("SELECT email FROM user.user_table WHERE email = %s", (email,))
    if cursor.fetchone():
        st.error('User already exists!')
        return False

    hashed_password = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt())
    encoded_password = base64.b64encode(hashed_password).decode('utf-8')
    activation_token = secrets.token_urlsafe(32)  # Generate a unique toke

    insert_query = """
        INSERT INTO user.user_table (email, username, password, role, is_active, activation_token) 
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    cursor.execute(insert_query, (email, username, encoded_password, role, False, activation_token))
    conn.commit()
    cursor.close()
    conn.close()

    send_activation_email(email, activation_token)

    st.success("Registration successful! You can now log in.")
    return True




# Function to display the registration page and handle user registration
def register_page():
    """Displays a registration form and handles user registration."""
    st.title("Register Page")

    # Input fields for registration
    username = st.text_input("Username")
    email = st.text_input("Email")
    # role = st.selectbox("Select Role", ["USER", "ADMIN"], index=0)
    # selected_projects = st.multiselect("Assign Projects", list(schema_value.keys()))
    password1 = st.text_input("Password", type="password")
    password2 = st.text_input("Confirm Password", type="password")

    if st.button("Register"):
        # Validate inputs
        if not username or not email or not password1 or not password2:
            st.error("Please fill in all fields.")
        elif password1 != password2:
            st.error("Passwords do not match.")
        else:
            # Call the register_new_user function to register the user in Snowflake
            if register_new_user(email, username, password1,'ADMIN'):
                st.success("Registration successful! You can now log in.")
                
                # Redirect to login page
                st.markdown(f'<meta http-equiv="refresh" content="0;url=/?page=login">', unsafe_allow_html=True)

                # st.experimental_set_query_params(page="login")
                # query_params = st.query_params()
                # st.write("Updated Query Params:", query_params)

                # st.rerun()

    # Navigation buttons
    if st.button("Login"):
        # st.experimental_set_query_params(page="login")
        # st.rerun()
        st.markdown(f'<meta http-equiv="refresh" content="0;url=/?page=login">', unsafe_allow_html=True)



def activate_account():
    """Activate the user account based on the verification token."""
    st.title("Account Activation")

    # Extract token from URL
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

    # Activate the user
    cursor.execute("UPDATE user.user_table SET is_active = %s, activation_token = NULL WHERE email = %s", (True, email))
    conn.commit()
    cursor.close()
    conn.close()

    st.success("Your account has been activated! You can now log in.")
    st.markdown(f'<meta http-equiv="refresh" content="2;url=/?page=login">', unsafe_allow_html=True)


# Function to generate JWT token
def generate_jwt(email, username, role):
    payload = {
        "email": email,
        "username": username,
        "role": role,
        "exp": datetime.utcnow() + timedelta(hours=1)  # Token expires in 1 hour
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)



# This will be used when role base value has updated
def store_user_in_session(user_data):
    st.session_state["logged_in"] = True
    st.session_state["user"] = user_data
    st.session_state["token"] = generate_jwt(user_data["email"], user_data["username"], user_data["role"])



# Check if user exists, password matches, and account is active
def check_user_login(email, password):
    # Connect to Snowflake
    conn = user_connect_to_snowflake()
    cursor = conn.cursor()

    # Query to get the user based on the email
    query = """
    SELECT email, username, password, role, is_active
    FROM user.user_table
    WHERE email = %s
    """
    cursor.execute(query, (email,))
    user = cursor.fetchone()  # Fetch the user record
    
    # Check if user exists and password matches
    if user:
        stored_hashed_password = base64.b64decode(user[2])  # user[2] is the password column
        is_active = user[4]  # user[4] is the is_active column

        if is_active:  # Only proceed if the account is active
            if bcrypt.checkpw(password.encode('utf-8'), stored_hashed_password):
                return {"email": user[0], "username": user[1], "role": user[3]}  # Password matches
            else:
                return None  # Incorrect password
        else:
            return "inactive"  # User exists but is inactive
    else:
        return None  # User not found


def login():
    """Displays a login form and handles authentication."""
    st.title("Login Page")
    
    # User inputs
    email = st.text_input("Email")
    password = st.text_input("Password", type="password")
    
    # Project selection
    project = st.selectbox("Select a Project", list(schema_value.keys()))

    # Handle login button click
    if st.button("Login"):
        user = check_user_login(email, password)
        
        if user == "inactive":
            st.error("Your account is not active. Please verify your email before logging in.")
        elif user:
            store_user_in_session(user)
            st.success(f"Welcome {user['username']}!")
            # Generate JWT token
            jwt_token = generate_jwt(user["email"], user["username"], user["role"])
            # If login is successful
            st.session_state["logged_in"] = True
            st.session_state["jwt_token"] = jwt_token
            st.session_state["selected_project"] = project  # Store selected project in session state
            st.session_state["schema"] = schema_value[project]

            # Preserve 'page' parameter in URL after login
            st.query_params["logged_in"] = "true"
            st.query_params["page"] = "main"
            st.rerun()  # Refresh the page after login

        else:
            # Display error if login fails
            st.error("Incorrect email or password")

    if st.button("Forgot Password"):
        st.markdown(f'<meta http-equiv="refresh" content="0;url=/?page=forgot_password">', unsafe_allow_html=True)



def logout():
    st.session_state.clear()
    st.success("Logged out successfully!")
    st.query_params["page"] = "login"
    st.rerun()


def is_authenticated():
    """Check if the user is authenticated."""
    if "logged_in" in st.session_state and st.session_state.get("logged_in", False):
        token = st.session_state.get("token")
        if token:
            # Optionally, you can verify the token here
            try:
                # Decode the token to check its validity
                decoded_token = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
                return True  # Valid token, user is authenticated
            except jwt.ExpiredSignatureError:
                st.warning("Session expired, please log in again.")
                logout()
            except jwt.InvalidTokenError:
                st.error("Invalid authentication token.")
                logout()
        else:
            return False  # No token found
    else:
        return False  # User is not logged in


# Function to decode JWT token
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
    """Generate a password reset token (JWT)."""
    payload = {
        "email": email,
        "exp": datetime.utcnow() + timedelta(minutes=15)  # Token expires in 15 minutes
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)


def send_reset_email(user_email, reset_token):
    """Send password reset email with the reset token link."""
    reset_link = f"http://18.116.237.208:8501/?page=reset_password&token={reset_token}"
    subject = "Password Reset Request"

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
                padding: 20px;
                border-radius: 8px;
                box-shadow: 0px 0px 10px rgba(0, 0, 0, 0.1);
                max-width: 500px;
                margin: auto;
            }}
            .btn {{
                display: inline-block;
                padding: 12px 20px;
                margin-top: 20px;
                font-size: 16px;
                color: #ffffff;
                background-color: #dc3545; /* Red color for reset warning */
                text-decoration: none;
                border-radius: 5px;
                font-weight: bold;
            }}
            .btn:hover {{
                background-color: #c82333;
            }}
            .footer {{
                margin-top: 20px;
                font-size: 12px;
                color: #666;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="email-box">
                <h2>Password Reset Request</h2>
                <p>We received a request to reset your password. Click the button below to proceed:</p>
                <a class="btn" href="{reset_link}" style="color: #fff">Reset My Password</a>
                <p>If the button above doesn't work, you can also use this link:</p>
                <p><a href="{reset_link}">{reset_link}</a></p>
                <p class="footer">If you did not request a password reset, please ignore this email.</p>
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
        msg.attach(MIMEText(body, 'html'))  # Use 'html' instead of 'plain'

        with smtplib.SMTP(EMAIL_HOST, EMAIL_PORT) as server:
            server.starttls()
            server.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
            server.sendmail(EMAIL_ADDRESS, user_email, msg.as_string())
        st.success("Password reset link sent to your email.")

    except Exception as e:
        print(f"Failed to send email: {e}")  # Replace with `st.error()` if using Streamlit

# Forgot Password page
def forgot_password():
    """Displays the forgot password page."""
    st.title("Forgot Password")

    email = st.text_input("Enter your email address")
    if st.button("Send Reset Link"):
        if not email:
            st.error("Email Field is Required")
            return
        # Check if the email exists in the user database (you may need to adjust this query)
        conn = user_connect_to_snowflake()
        cursor = conn.cursor()
        cursor.execute("SELECT email FROM user.user_table WHERE email = %s", (email,))
        user = cursor.fetchone()

        if user:
            reset_token = generate_reset_token(email)
            send_reset_email(email, reset_token)
            # st.rerun()
            # email = ""
        else:
            st.error("Email not found in the system.")
        
        cursor.close()
        conn.close()

def decode_reset_token(reset_token):
    """Decode the reset token to get user email."""
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
    """Update the user's password in the database."""
    hashed_password = bcrypt.hashpw(new_password.encode('utf-8'), bcrypt.gensalt())
    encoded_hashed_password = base64.b64encode(hashed_password).decode('utf-8')

    conn = user_connect_to_snowflake()
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE user.user_table SET password = %s WHERE email = %s
    """, (encoded_hashed_password, email))
    print("query executed")
    conn.commit()
    cursor.close()
    conn.close()
    st.success("Password updated successfully!")


# Reset Password page
def reset_password():
    """Displays the reset password form."""
    st.title("Reset Password")

    # Get reset token from URL query parameters
    query_params = st.query_params
    reset_token = query_params.get("token", None)  # Fetch the token from URL (e.g., ?token=xyz)

    if not reset_token:
        st.error("Reset token is missing or invalid.")
        return  # Exit the function if no token is provided in the URL

    # Password fields
    new_password = st.text_input("Enter your new password", type="password")
    confirm_password = st.text_input("Confirm your new password", type="password")
    
    if st.button("Reset Password"):
        if new_password != confirm_password:
            st.error("Passwords do not match.")
        else:
            # Decode the reset token to get the email
            email = decode_reset_token(reset_token)
            if email:
                update_user_password(email, new_password)
                st.success("Password reset successful.")
                # Optionally, redirect to login page after password reset
                st.markdown(f'<meta http-equiv="refresh" content="0;url=/?page=login">', unsafe_allow_html=True)
            else:
                st.error("Invalid reset token.")

def generate_change_password_token(email):
    """Generate a JWT token for changing password."""
    payload = {
        "email": email,
        "exp": datetime.utcnow() + timedelta(minutes=30)  # Token expires in 30 minutes
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)



def send_change_password_email(email):
    """Send a password change email with a secure link."""
    token = generate_change_password_token(email)
    change_link = f"http://18.116.237.208:8501/?page=change_password&token={token}"
    
    subject = "Change Your Password"

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
                padding: 20px;
                border-radius: 8px;
                box-shadow: 0px 0px 10px rgba(0, 0, 0, 0.1);
                max-width: 500px;
                margin: auto;
            }}
            .btn {{
                display: inline-block;
                padding: 12px 20px;
                margin-top: 20px;
                font-size: 16px;
                color: #fff;
                background-color: #007bff; /* Blue for security action */
                text-decoration: none;
                border-radius: 5px;
                font-weight: bold;
            }}
            .btn:hover {{
                background-color: #0056b3;
                color: #fff;
            }}
            .footer {{
                margin-top: 20px;
                font-size: 12px;
                color: #666;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="email-box">
                <h2>Password Change Request</h2>
                <p>We received a request to change your password. Click the button below to proceed:</p>
                <a class="btn" href="{change_link}" style="color: #fff">Change My Password</a>
                <p>If the button above doesn't work, you can also use this link:</p>
                <p><a href="{change_link}">{change_link}</a></p>
                <p class="footer">This link is valid for <strong>30 minutes</strong>. If you did not request this, please ignore this email.</p>
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
        msg.attach(MIMEText(body, "html"))  # Use 'html' instead of 'plain'

        with smtplib.SMTP(EMAIL_HOST, EMAIL_PORT) as server:
            server.starttls()
            server.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
            server.sendmail(EMAIL_ADDRESS, email, msg.as_string())

        st.success("Password change link sent to your email.")  # Keep success message
    except Exception as e:
        st.error(f"Failed to send email: {e}")


def verify_change_password_token(token):
    """Verify JWT token and extract the email."""
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
    st.title("Change Password")

    # Extract token from URL query parameters
    query_params = st.query_params
    token = query_params.get("token", None)

    if not token:
        st.error("Invalid request. No change password token found.")
        return

    email = verify_change_password_token(token)
    if not email:
        return

    # Display change password form
    current_password = st.text_input("Enter your current password", type="password")
    new_password = st.text_input("Enter your new password", type="password")
    confirm_password = st.text_input("Confirm your new password", type="password")

    if st.button("Change Password"):
        if not current_password or not new_password or not confirm_password:
            st.error("All fields are required.")
            return
        
        if new_password != confirm_password:
            st.error("Passwords do not match.")
            return
        
        # Verify current password
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

        # Hash the new password and update in Snowflake
        hashed_password = bcrypt.hashpw(new_password.encode("utf-8"), bcrypt.gensalt())
        encoded_password = base64.b64encode(hashed_password).decode("utf-8")

        cursor.execute("UPDATE user.user_table SET password = %s WHERE email = %s", (encoded_password, email))
        conn.commit()
        cursor.close()
        conn.close()

        st.success("Password changed successfully. You can now log in.")
        st.markdown(f'<meta http-equiv="refresh" content="0;url=/?page=login">', unsafe_allow_html=True)



def verify_user(email, current_password):
    """Verify the user's current password."""
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
    """Update the user password in Snowflake."""
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
    st.title("Change Password")

    # Initialize session state variables only once
    if "form_submitted" not in st.session_state:
        st.session_state["form_submitted"] = False
        st.session_state["current_password"] = ""
        st.session_state["new_password"] = ""
        st.session_state["confirm_password"] = ""

    try:
        with st.form(key="change_password_form", clear_on_submit=False):
            current_password = st.text_input(
                "Current Password",
                type="password",
                key="current_pwd_input"
            )
            new_password = st.text_input(
                "New Password",
                type="password",
                key="new_pwd_input"
            )
            confirm_password = st.text_input(
                "Confirm New Password",
                type="password",
                key="confirm_pwd_input"
            )

            submit_button = st.form_submit_button("Update Password")

            if submit_button:
                st.session_state["form_submitted"] = True  # Mark submission
                st.session_state["current_password"] = current_password
                st.session_state["new_password"] = new_password
                st.session_state["confirm_password"] = confirm_password

    except Exception as e:
        st.error(f"An unexpected error occurred: {str(e)}")
        logger.error(f"Unexpected error in change_password: {str(e)}")
        return

    # Check form submission state AFTER the form
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

        # Verify current password
        logger.debug("Verifying user password")
        if not verify_user(email, st.session_state["current_password"]):
            st.error("Current password is incorrect")
            logger.debug("Verification failed: Incorrect current password")
            return

        # Update password
        logger.debug("Attempting to update password")
        success = update_change_user_password(email, st.session_state["new_password"])
        if success:
            st.success("Password updated successfully!")
            # Reset session state to clear form inputs
            st.session_state["form_submitted"] = False
            st.session_state["current_password"] = ""
            st.session_state["new_password"] = ""
            st.session_state["confirm_password"] = ""
            logger.debug("Password update successful")
        else:
            st.error("Failed to update password")
            logger.debug("Password update failed")

    # Back button
    if st.button("Login"):
        for key in ["current_password", "new_password", "confirm_password", "form_submitted"]:
            if key in st.session_state:
                del st.session_state[key]
        st.experimental_set_query_params(page="main")
        st.rerun()