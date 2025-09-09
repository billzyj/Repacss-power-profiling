#!/usr/bin/env python3
"""
Setup script for REPACSS Power Measurement Client
"""

import os
import shutil
import sys
from pathlib import Path


def create_config_file():
    """Create config.py from template if it doesn't exist"""
    template_path = Path("src/config_template.py")
    config_path = Path("src/config.py")
    
    if config_path.exists():
        print("✓ config.py already exists")
        return
    
    if not template_path.exists():
        print("❌ config_template.py not found")
        return
    
    try:
        shutil.copy(template_path, config_path)
        print("✓ Created src/config.py from template")
        print("⚠️  Please edit src/config.py with your actual database and SSH credentials")
    except Exception as e:
        print(f"❌ Error creating config.py: {e}")


def check_dependencies():
    """Check if required dependencies are installed"""
    required_packages = [
        "psycopg2-binary",
        "paramiko", 
        "sshtunnel",
        "pandas",
        "openpyxl"
    ]
    
    missing_packages = []
    
    for package in required_packages:
        try:
            __import__(package.replace("-", "_"))
            print(f"✓ {package}")
        except ImportError:
            missing_packages.append(package)
            print(f"❌ {package} (missing)")
    
    if missing_packages:
        print(f"\n⚠️  Missing packages: {', '.join(missing_packages)}")
        print("Run: pip install -r requirements.txt")
        return False
    
    return True


def check_gitignore():
    """Check if config.py is in .gitignore"""
    gitignore_path = Path(".gitignore")
    
    if not gitignore_path.exists():
        print("❌ .gitignore not found")
        return False
    
    with open(gitignore_path, 'r') as f:
        content = f.read()
    
    if "src/config.py" in content:
        print("✓ config.py is in .gitignore")
        return True
    else:
        print("⚠️  config.py is not in .gitignore")
        print("Adding src/config.py to .gitignore...")
        
        with open(gitignore_path, 'a') as f:
            f.write("\n# Configuration files with sensitive data\nsrc/config.py\n")
        
        print("✓ Added src/config.py to .gitignore")
        return True


def check_excel_reporting():
    """Check if Excel reporting dependencies are available"""
    try:
        import pandas as pd
        import openpyxl
        print("✓ Excel reporting dependencies available")
        return True
    except ImportError as e:
        print(f"❌ Excel reporting dependencies missing: {e}")
        print("Install with: pip install pandas openpyxl")
        return False


def main():
    """Main setup function"""
    print("🚀 REPACSS Power Measurement Client Setup")
    print("=" * 50)
    
    # Check dependencies
    print("\n📦 Checking dependencies...")
    deps_ok = check_dependencies()
    
    # Check Excel reporting
    print("\n📊 Checking Excel reporting capabilities...")
    excel_ok = check_excel_reporting()
    
    # Create config file
    print("\n⚙️  Setting up configuration...")
    create_config_file()
    
    # Check gitignore
    print("\n🔒 Checking security...")
    check_gitignore()
    
    print("\n" + "=" * 50)
    if deps_ok and excel_ok:
        print("✅ Setup completed successfully!")
        print("\n📝 Next steps:")
        print("1. Edit src/config.py with your database and SSH credentials")
        print("2. Test your connection: python src/test_connection.py")
        print("3. Run examples: python src/example_usage.py")
        print("4. Generate power metrics report: python src/run_public_queries.py")
    elif deps_ok:
        print("⚠️  Setup completed with warnings")
        print("Excel reporting not available - install pandas and openpyxl")
        print("\n📝 Next steps:")
        print("1. Edit src/config.py with your database and SSH credentials")
        print("2. Install Excel dependencies: pip install pandas openpyxl")
        print("3. Test your connection: python src/test_connection.py")
        print("4. Run examples: python src/example_usage.py")
    else:
        print("⚠️  Setup completed with warnings")
        print("Please install missing dependencies before proceeding")
    
    print("\n📚 For more information, see README.md and USAGE_GUIDE.md")


if __name__ == "__main__":
    main()
