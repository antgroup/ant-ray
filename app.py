import streamlit as st
import requests
import zipfile
import io
import os
import tempfile
import mimetypes

def parse_github_artifact_params(run_id, artifact_id):
    """Create artifact info from run_id and artifact_id"""
    if not run_id or not artifact_id:
        return None
    return {
        'owner': 'antgroup',  # Hardcoded for ant-ray
        'repo': 'ant-ray',
        'run_id': run_id,
        'artifact_id': artifact_id
    }

def create_temp_dir():
    if 'temp_dir' not in st.session_state:
        st.session_state.temp_dir = tempfile.mkdtemp()
    return st.session_state.temp_dir

def download_and_extract_zip(api_url):
    try:
        # Get GitHub token from session state
        github_token = st.session_state.get('github_token')
        if not github_token:
            return None

        # Configure API request
        headers = {
            "Authorization": f"token {github_token}",
            "Accept": "application/vnd.github.v3+json"
        }

        # Download artifact
        session = requests.Session()
        response = session.get(api_url, headers=headers, stream=True)
        response.raise_for_status()
        
        temp_dir = create_temp_dir()
        
        # Download with progress bar
        file_size = int(response.headers.get('content-length', 0))
        progress_bar = st.progress(0)
        block_size = 1024  # 1 Kibibyte
        
        # Create a BytesIO object to store the downloaded content
        content = io.BytesIO()
        
        if file_size:  # Only show progress bar if we know the file size
            progress_text = st.empty()
            downloaded = 0
            for data in response.iter_content(block_size):
                downloaded += len(data)
                content.write(data)
                progress = int((downloaded / file_size) * 100)
                progress_bar.progress(progress)
                progress_text.text(f"Downloading: {progress}%")
        else:
            # If file size is unknown, just download without progress
            content.write(response.content)
        
        progress_bar.empty()  # Remove progress bar after download
        
        try:
            # Extract zip content
            with zipfile.ZipFile(content) as zip_ref:
                zip_ref.extractall(temp_dir)
            return temp_dir
        except zipfile.BadZipFile:
            st.error("The downloaded file is not a valid zip file.")
            return None
            
    except requests.RequestException as e:
        st.error(f"Error downloading file: {str(e)}")
        return None
    except Exception as e:
        st.error(f"Unexpected error: {str(e)}")
        return None

def get_file_structure(directory):
    file_list = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            # Skip JSON files
            if file.endswith('.json'):
                continue
            full_path = os.path.join(root, file)
            relative_path = os.path.relpath(full_path, directory)
            file_list.append(relative_path)
    return sorted(file_list)

def get_file_icon(file_path):
    """Get an emoji icon based on file type"""
    if file_path.endswith('.html'):
        return "🌐"
    elif file_path.endswith('.zip'):
        return "📦"
    elif file_path.endswith(('.png', '.jpg', '.jpeg', '.gif')):
        return "🖼️"
    elif file_path.endswith(('.md', '.txt')):
        return "📄"
    elif file_path.endswith('.pdf'):
        return "📑"
    elif file_path.endswith(('.py', '.js', '.java', '.cpp')):
        return "👨‍💻"
    else:
        return "📎"

def format_file_path(file_path):
    """Format file path for better display"""
    parts = file_path.split('/')
    if len(parts) == 1:
        return f"{get_file_icon(file_path)} {parts[0]}"
    else:
        return f"📁 {'/'.join(parts[:-1])}/\n   {get_file_icon(parts[-1])} {parts[-1]}"

def is_zip_file(file_path):
    return file_path.endswith('.zip') or mimetypes.guess_type(file_path)[0] == 'application/zip'

def get_file_bytes(file_path):
    with open(file_path, 'rb') as f:
        return f.read()

def main():
    # Remove the default page config and set to wide layout without toolbar
    st.set_page_config(
        layout="wide",
        initial_sidebar_state="expanded",
        menu_items={},  # This removes the toolbar menu
    )
    
    # Hide streamlit default elements using CSS
    hide_streamlit_style = """
        <style>
            #MainMenu {visibility: hidden;}
            header {visibility: hidden;}
            footer {visibility: hidden;}
        </style>
    """
    st.markdown(hide_streamlit_style, unsafe_allow_html=True)
    
    # Get run_id and artifact_id from query parameters
    query_params = st.query_params
    run_id = query_params.get("run_id")
    artifact_id = query_params.get("artifact_id")
    
    # Create the main layout with sidebar
    with st.sidebar:
        # Clear button at the top of sidebar
        if st.button("Clear Cache and Temp Files", use_container_width=True):
            if 'temp_dir' in st.session_state:
                import shutil
                try:
                    shutil.rmtree(st.session_state.temp_dir)
                except:
                    pass
                del st.session_state.temp_dir
                if 'github_token' in st.session_state:
                    del st.session_state.github_token
            st.rerun()

        st.divider()
        
        if not run_id or not artifact_id:
            st.error("Missing run_id or artifact_id. Add '?run_id=XXX&artifact_id=YYY' to the page URL.")
            return
            
        # For debugging
        st.write(f"Run ID: {run_id}")
        st.write(f"Artifact ID: {artifact_id}")
        
        # Get GitHub token if needed
        if 'github_token' not in st.session_state:
            github_token = os.environ.get("GITHUB_TOKEN_X") or st.text_input(
                "Enter GitHub Token (needed for downloading artifacts)",
                type="password",
                key="token_input"
            )
            if github_token:
                st.session_state.github_token = github_token
        
        # Process artifacts and show file structure
        if 'github_token' in st.session_state and 'temp_dir' not in st.session_state:
            # Create API URL from run_id and artifact_id
            artifact_info = parse_github_artifact_params(run_id, artifact_id)
            if artifact_info:
                api_url = f"https://api.github.com/repos/{artifact_info['owner']}/{artifact_info['repo']}/actions/artifacts/{artifact_info['artifact_id']}/zip"
                temp_dir = download_and_extract_zip(api_url)
                if temp_dir:
                    st.session_state.temp_dir = temp_dir

        # Show file structure if we have files
        if 'temp_dir' in st.session_state:
            files = get_file_structure(st.session_state.temp_dir)
            
            if not files:
                st.sidebar.info("No viewable files found")
                return

            # Group files by type
            html_files = [f for f in files if f.endswith('.html')]
            other_files = [f for f in files if not f.endswith('.html')]

            # Show HTML files first with preview option
            if html_files:
                st.session_state.selected_file = html_files[0]

            # Show other files as download buttons
            if other_files:
                st.sidebar.markdown("#### 📥 Downloads")
                for file in other_files:
                    full_path = os.path.join(st.session_state.temp_dir, file)
                    file_bytes = get_file_bytes(full_path)
                    st.sidebar.download_button(
                        label=f"{get_file_icon(file)} {os.path.basename(file)}",
                        data=file_bytes,
                        file_name=os.path.basename(file),
                        mime=mimetypes.guess_type(file)[0] or 'application/octet-stream',
                        use_container_width=True
                    )

    # Main content area - only for HTML preview
    if 'temp_dir' in st.session_state and 'selected_file' in st.session_state:
        full_path = os.path.join(st.session_state.temp_dir, st.session_state.selected_file)
        
        if full_path.endswith('.html'):
            try:
                with open(full_path, 'r', encoding='utf-8') as f:
                    html_content = f.read()
                    
                    # Inject CSS to remove padding from all main containers and set iframe height to full viewport
                    st.markdown("""
                    <style>
                        /* Remove default padding on the main app container */
                        [data-testid="stAppViewContainer"],
                        .block-container,
                        section.main > div:first-child {
                            padding: 0 !important;
                            margin: 0 !important;
                        }
                        
                        /* Make the iframe take full viewport height */
                        iframe {
                            height: 100vh !important;
                            margin: 0 !important;
                            padding: 0 !important;
                        }
                    </style>
                    """, unsafe_allow_html=True)
                    
                    # Render the HTML preview filling the entire right side
                    st.components.v1.html(
                        html_content, 
                        height=0,  # Allow CSS to take over the height
                        scrolling=True
                    )
            except Exception as e:
                st.error(f"Error reading HTML file: {str(e)}")

if __name__ == "__main__":
    main()