#!/usr/bin/env python3
"""
SAWGraph Automatic Setup Script
Run this file to automatically set up everything:
    python setup_sawgraph.py
"""

import os
import sys
import subprocess
import platform

def create_requirements_file():
    """Create requirements.txt file"""
    requirements = """streamlit>=1.28.0
streamlit-folium>=0.15.0
folium>=0.14.0
geopandas>=0.14.0
pandas>=2.0.0
shapely>=2.0.0
SPARQLWrapper>=2.0.0
rdflib>=6.3.0
branca>=0.6.0"""
    
    with open('requirements.txt', 'w') as f:
        f.write(requirements)
    print("✅ Created requirements.txt")

def create_test_app():
    """Create a simple test app"""
    test_code = '''import streamlit as st
import folium
from streamlit_folium import st_folium

st.set_page_config(page_title="SAWGraph Test", page_icon="🗺️")
st.title("🗺️ SAWGraph Setup Test")
st.success("✅ Setup successful! Streamlit is working!")

# Create a simple map
m = folium.Map(location=[45.2538, -69.4455], zoom_start=7)
folium.Marker([45.2538, -69.4455], popup="Maine", tooltip="Center of Maine").add_to(m)

st.subheader("Test Map")
st_folium(m, width=700, height=400)

st.info("""
**Next Steps:**
1. Copy your full app.py code from the artifact
2. Replace this test file with your actual app.py
3. Run: streamlit run app.py
""")
'''
    
    # Only create test app if app.py doesn't exist
    if not os.path.exists('app.py'):
        with open('app.py', 'w') as f:
            f.write(test_code)
        print("✅ Created test app.py (replace with your actual app code)")
    else:
        print("✅ app.py already exists")

def setup_virtual_environment():
    """Set up virtual environment and install packages"""
    print("\n🔧 Setting up virtual environment...")
    
    # Check if venv exists
    venv_exists = os.path.exists('venv')
    
    if not venv_exists:
        # Create virtual environment
        subprocess.run([sys.executable, '-m', 'venv', 'venv'], check=True)
        print("✅ Created virtual environment")
    else:
        print("✅ Virtual environment already exists")
    
    # Determine the pip path based on OS
    if platform.system() == 'Windows':
        pip_path = os.path.join('venv', 'Scripts', 'pip')
        python_path = os.path.join('venv', 'Scripts', 'python')
        activate_cmd = os.path.join('venv', 'Scripts', 'activate.bat')
        activate_instruction = f"venv\\Scripts\\activate"
    else:
        pip_path = os.path.join('venv', 'bin', 'pip')
        python_path = os.path.join('venv', 'bin', 'python')
        activate_cmd = os.path.join('venv', 'bin', 'activate')
        activate_instruction = "source venv/bin/activate"
    
    print("\n📦 Installing packages...")
    
    # Upgrade pip first
    subprocess.run([pip_path, 'install', '--upgrade', 'pip'], 
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    
    # Install requirements
    try:
        result = subprocess.run([pip_path, 'install', '-r', 'requirements.txt'], 
                              capture_output=True, text=True)
        if result.returncode == 0:
            print("✅ All packages installed successfully")
        else:
            print("⚠️ Some packages may have issues, but continuing...")
            print("   You can manually install missing packages later")
    except Exception as e:
        print(f"⚠️ Installation issue: {e}")
        print("   Try manually: pip install -r requirements.txt")
    
    return activate_instruction, python_path

def main():
    print("=" * 50)
    print("🚀 SAWGraph Streamlit App - Automatic Setup")
    print("=" * 50)
    
    # Step 1: Create requirements.txt
    create_requirements_file()
    
    # Step 2: Create test app
    create_test_app()
    
    # Step 3: Set up environment and install
    activate_instruction, python_path = setup_virtual_environment()
    
    print("\n" + "=" * 50)
    print("✅ SETUP COMPLETE!")
    print("=" * 50)
    
    print("\n📋 To run the app:")
    print(f"1. Activate environment: {activate_instruction}")
    print("2. Run app: streamlit run app.py")
    
    print("\n💡 Or run directly:")
    if platform.system() == 'Windows':
        print(f"   venv\\Scripts\\streamlit run app.py")
    else:
        print(f"   venv/bin/streamlit run app.py")
    
    print("\n" + "=" * 50)
    
    # Ask if user wants to run the app now
    response = input("\n🚀 Start the app now? (y/n): ").lower().strip()
    if response == 'y' or response == 'yes':
        print("\n🌟 Starting Streamlit app...")
        print("   (Press Ctrl+C to stop)")
        print("-" * 50)
        
        if platform.system() == 'Windows':
            streamlit_path = os.path.join('venv', 'Scripts', 'streamlit')
        else:
            streamlit_path = os.path.join('venv', 'bin', 'streamlit')
        
        try:
            subprocess.run([streamlit_path, 'run', 'app.py'])
        except KeyboardInterrupt:
            print("\n\n✅ App stopped")
        except FileNotFoundError:
            print("⚠️ Streamlit not found. Please activate venv and run manually:")
            print(f"   {activate_instruction}")
            print("   streamlit run app.py")
    else:
        print("\n✅ Setup complete! Run the app when ready with:")
        print(f"   {activate_instruction}")
        print("   streamlit run app.py")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌ Error: {e}")
        print("\nPlease try manual setup:")
        print("1. pip install -r requirements.txt")
        print("2. streamlit run app.py")
        sys.exit(1)