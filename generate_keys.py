import streamlit_authenticator as stauth

# Ganti ini dengan password yang Anda inginkan
passwords_to_hash = ["password123", "passXYZ"] 

hashed_passwords = stauth.Hasher(passwords_to_hash).generate()
print(hashed_passwords)
