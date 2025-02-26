import os
import jwt
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

def user_connect_to_snowflake():
    return snowflake.connector.connect(
        user=os.getenv('user'),
        password=os.getenv('password'),
        account=os.getenv('account'),
        warehouse=os.getenv('warehouse'),
        database=os.getenv('database'),
        schema='user',
        role=os.getenv('role')
    )


schema_value = {'TUCSON': 'tucson_bus','TUCSON RAIL': 'tucson_rail','VTA': 'vta_bus', 'UTA': 'uta_rail'}


def send_activation_email(email, activation_token):
    """Send an account activation email with a secure token."""
    activation_link = f"http://18.116.237.208:8501/?page=activate&token={activation_token}"
    subject = "Activate Your Account"
    body = f"Click the link to activate your account: {activation_link}\n"

    try:
        msg = MIMEMultipart()
        msg['From'] = EMAIL_ADDRESS
        msg['To'] = email
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'plain'))

        with smtplib.SMTP(EMAIL_HOST, EMAIL_PORT) as server:
            server.starttls()
            server.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
            server.sendmail(EMAIL_ADDRESS, email, msg.as_string())

        st.success("Activation email sent! Please check your inbox.")
    except Exception as e:
        st.error(f"Failed to send email: {e}")


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
        # st.success(f"User {username} created successfully! Activation email sent.")
        return True
    except Exception as e:
        st.error(f"Failed to create user: {e}")
        return False
    finally:
        cursor.close()
        conn.close()

# def create_new_user_page():
#     """Displays a form for admins to create new users."""
#     st.title("Create New User (Admin Only)")

#     # Check if the current user is an admin
#     if "user" not in st.session_state or st.session_state["user"].get("role", "").lower() != "admin":
#         st.error("You must be an admin to access this page.")
#         st.stop()

#     # Initialize session state for form inputs
#     if "create_user_username" not in st.session_state:
#         st.session_state["create_user_username"] = ""
#     if "create_user_email" not in st.session_state:
#         st.session_state["create_user_email"] = ""
#     if "create_user_role" not in st.session_state:
#         st.session_state["create_user_role"] = "USER"
#     if "create_user_password" not in st.session_state:
#         st.session_state["create_user_password"] = ""
#     if "create_user_confirm_password" not in st.session_state:
#         st.session_state["create_user_confirm_password"] = ""

#     with st.form(key="create_new_user_form"):
#         # Form inputs tied to session state
#         st.session_state["create_user_username"] = st.text_input("Username", value=st.session_state["create_user_username"])
#         st.session_state["create_user_email"] = st.text_input("Email", value=st.session_state["create_user_email"])
#         st.session_state["create_user_role"] = st.selectbox("Select Role", ["USER", "ADMIN"], index=["USER", "ADMIN"].index(st.session_state["create_user_role"]))
#         st.session_state["create_user_password"] = st.text_input("Password", type="password", value=st.session_state["create_user_password"])
#         st.session_state["create_user_confirm_password"] = st.text_input("Confirm Password", type="password", value=st.session_state["create_user_confirm_password"])
        
#         submit_button = st.form_submit_button(label="ADD User")

#         if submit_button:
#             username = st.session_state["create_user_username"]
#             email = st.session_state["create_user_email"]
#             role = st.session_state["create_user_role"]
#             password = st.session_state["create_user_password"]
#             confirm_password = st.session_state["create_user_confirm_password"]

#             if not username or not email or not password or not confirm_password:
#                 st.error("Please fill in all fields.")
#             elif password != confirm_password:
#                 st.error("Passwords do not match.")
#             else:
#                 if create_new_user(email, username, password, role):
#                     # Clear form after successful creation
#                     st.session_state["create_user_username"] = ""
#                     st.session_state["create_user_email"] = ""
#                     st.session_state["create_user_role"] = "USER"
#                     st.session_state["create_user_password"] = ""
#                     st.session_state["create_user_confirm_password"] = ""

#     # Navigation back to main page (outside the form)
#     if st.button("Back to Main Page"):
#         st.experimental_set_query_params(page="main")
#         st.experimental_rerun()


def create_new_user_page():
    """Displays a form for admins to create new users."""
    st.title("Create New User (Admin Only)")

    # Check if the current user is an admin
    if "user" not in st.session_state or st.session_state["user"].get("role", "").lower() != "admin":
        st.error("You must be an admin to access this page.")
        st.stop()

    # Initialize session state for form inputs
    if "form_submitted" not in st.session_state:
        st.session_state["form_submitted"] = False

    with st.form(key="create_new_user_form"):
        # Form inputs
        username = st.text_input("Username", value=st.session_state.get("create_user_username", ""))
        email = st.text_input("Email", value=st.session_state.get("create_user_email", ""))
        role = st.selectbox("Select Role", ["USER", "ADMIN"], index=["USER", "ADMIN"].index(st.session_state.get("create_user_role", "USER")))
        password = st.text_input("Password", type="password", value=st.session_state.get("create_user_password", ""))
        confirm_password = st.text_input("Confirm Password", type="password", value=st.session_state.get("create_user_confirm_password", ""))
        
        submit_button = st.form_submit_button(label="ADD User")

        if submit_button:
            st.session_state["form_submitted"] = True
            st.session_state["create_user_username"] = username
            st.session_state["create_user_email"] = email
            st.session_state["create_user_role"] = role
            st.session_state["create_user_password"] = password
            st.session_state["create_user_confirm_password"] = confirm_password

            if not username or not email or not password or not confirm_password:
                st.error("Please fill in all fields.")
            elif password != confirm_password:
                st.error("Passwords do not match.")
            else:
                if create_new_user(email, username, password, role):
                    st.success("User created successfully! Activation email sent.")
                    # Clear form after successful creation
                    st.session_state["create_user_username"] = ""
                    st.session_state["create_user_email"] = ""
                    st.session_state["create_user_role"] = "USER"
                    st.session_state["create_user_password"] = ""
                    st.session_state["create_user_confirm_password"] = ""
                    st.session_state["form_submitted"] = False
    # Navigation back to main page (outside the form)
    if st.button("Back to Main Page"):
        st.experimental_set_query_params(page="main")
        st.experimental_rerun()



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
    cursor.execute(insert_query, (email, username, encoded_password, role, True, activation_token))
    conn.commit()
    cursor.close()
    conn.close()

    # send_activation_email(email, activation_token)

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
                # query_params = st.experimental_get_query_params()
                # st.write("Updated Query Params:", query_params)

                # st.experimental_rerun()

    # Navigation buttons
    if st.button("Go to Login Page"):
        # st.experimental_set_query_params(page="login")
        # st.experimental_rerun()
        st.markdown(f'<meta http-equiv="refresh" content="0;url=/?page=login">', unsafe_allow_html=True)



def activate_account():
    """Activate the user account based on the verification token."""
    st.title("Account Activation")

    # Extract token from URL
    query_params = st.experimental_get_query_params()
    token = query_params.get("token", [None])[0]

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



# Streamlit login page
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


            # If login is successful
            st.session_state["logged_in"] = True
            st.session_state["selected_project"] = project  # Store selected project in session state
            st.session_state["schema"] = schema_value[project]

            # Preserve 'page' parameter in URL after login
            st.experimental_set_query_params(logged_in="true", page='main', token=st.session_state["jwt_token"])  
            st.experimental_rerun()  # Refresh the page after login

        else:
            # Display error if login fails
            st.error("Incorrect email or password")
    
    # Button to go to register page
    # if st.button("Register"):
    #     st.markdown(f'<meta http-equiv="refresh" content="0;url=/?page=register">', unsafe_allow_html=True)

    if st.button("Forgot Password"):
        st.markdown(f'<meta http-equiv="refresh" content="0;url=/?page=forgot_password">', unsafe_allow_html=True)


def logout():
    st.session_state.clear()
    st.success("Logged out successfully!")
    st.experimental_set_query_params(page="login")
    st.experimental_rerun()


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
    body = f"Click the link to reset your password: {reset_link}"

    # Send email (Using SMTP)
    try:
        msg = MIMEMultipart()
        msg['From'] = EMAIL_ADDRESS
        msg['To'] = user_email
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'plain'))
        
        # SMTP connection and email sending
        with smtplib.SMTP(EMAIL_HOST, EMAIL_PORT) as server:
            server.starttls()  # Secure the connection
            server.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
            server.sendmail(EMAIL_ADDRESS, user_email, msg.as_string())
        
        st.success("Password reset link sent to your email.")
    except Exception as e:
        st.error(f"Failed to send email: {e}")


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
            # st.experimental_rerun()
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
    query_params = st.experimental_get_query_params()
    reset_token = query_params.get("token", [None])[0]  # Fetch the token from URL (e.g., ?token=xyz)

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
    body = f"Click the link to change your password: {change_link}\nThis link is valid for 30 minutes."

    # # Debug: Check if the function is triggered
    # st.write("Sending email to:", email)
    # st.write("Generated link:", change_link)

    try:
        msg = MIMEMultipart()
        msg["From"] = EMAIL_ADDRESS
        msg["To"] = email
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "plain"))
        
        with smtplib.SMTP(EMAIL_HOST, EMAIL_PORT) as server:
            server.starttls()
            server.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
            server.sendmail(EMAIL_ADDRESS, email, msg.as_string())
        
        st.success("Password change link sent to your email.")
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
    query_params = st.experimental_get_query_params()
    token = query_params.get("token", [None])[0]

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
    if st.button("Back to Main Page"):
        for key in ["current_password", "new_password", "confirm_password", "form_submitted"]:
            if key in st.session_state:
                del st.session_state[key]
        st.experimental_set_query_params(page="main")
        st.experimental_rerun()