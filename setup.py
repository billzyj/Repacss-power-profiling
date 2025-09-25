#!/usr/bin/env python3
"""
Enhanced setup script for REPACSS Power Measurement Client
Supports new structure with CLI, environment variables, and improved configuration
"""

import os
import shutil
import sys
from pathlib import Path


def create_env_file():
    """Create .env file from template if it doesn't exist"""
    template_path = Path("src/database/config/env.template")
    env_path = Path("src/database/config/.env")
    
    if env_path.exists():
        print("✓ .env file already exists")
        return
    
    if not template_path.exists():
        print("❌ env.template not found")
        return
    
    try:
        shutil.copy(template_path, env_path)
        print("✓ Created .env file from template")
        print("⚠️  Please edit .env with your actual database and SSH credentials")
    except Exception as e:
        print(f"❌ Error creating .env file: {e}")




def check_dependencies():
    """Check if required dependencies are installed"""
    required_packages = [
        "psycopg2-binary",
        "paramiko", 
        "sshtunnel",
        "pandas",
        "openpyxl",
        "click",
        "matplotlib"
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
        print("Or install individually: pip install click psycopg2-binary")
        
        # Try to install missing packages automatically
        try:
            import subprocess
            print(f"\n🔧 Attempting to install missing packages...")
            result = subprocess.run([sys.executable, "-m", "pip", "install"] + missing_packages, 
                                  capture_output=True, text=True)
            if result.returncode == 0:
                print("✅ Successfully installed missing packages!")
                return True
            else:
                print(f"❌ Failed to install packages: {result.stderr}")
        except Exception as e:
            print(f"❌ Could not install packages automatically: {e}")
        
        return False
    
    return True


def check_gitignore():
    """Check if sensitive files are in .gitignore"""
    gitignore_path = Path(".gitignore")
    
    if not gitignore_path.exists():
        print("❌ .gitignore not found")
        return False
    
    with open(gitignore_path, 'r') as f:
        content = f.read()
    
    # Check for .env files
    env_ok = ".env" in content and "src/database/config/.env" in content
    if env_ok:
        print("✓ .env files are in .gitignore")
    else:
        print("⚠️  .env files are not in .gitignore")
        print("Adding .env files to .gitignore...")
        
        with open(gitignore_path, 'a') as f:
            f.write("\n# Environment variables with sensitive data\n.env\nsrc/database/config/.env\n")
        
        print("✓ Added .env files to .gitignore")
    
    return env_ok


def check_new_structure():
    """Check if new directory structure exists"""
    required_dirs = [
        "src/cli",
        "src/cli/commands", 
        "src/analysis",
        "src/reporting",
        "src/utils",
        "src/database/config",
        "tests/unit",
        "tests/integration",
        "tests/fixtures"
    ]
    
    missing_dirs = []
    
    for dir_path in required_dirs:
        if not Path(dir_path).exists():
            missing_dirs.append(dir_path)
    
    if missing_dirs:
        print(f"❌ Missing directories: {', '.join(missing_dirs)}")
        print("Run: mkdir -p " + " ".join(missing_dirs))
        return False
    
    print("✓ New directory structure exists")
    return True


def check_new_modules():
    """Check if new modules exist"""
    required_files = [
        "src/cli/main.py",
        "src/cli/commands/analyze.py",
        "src/cli/commands/report.py", 
        "src/cli/commands/test.py",
        "src/analysis/power.py",
        "src/analysis/energy.py",
        "src/utils/conversions.py",
        "src/utils/data_processing.py",
        "src/reporting/excel.py",
        "src/reporting/formats.py",
        "src/database/config/config.py",
        "src/database/connection_pool.py",
        "src/queries/manager.py",
        "src/scripts/run_compute_power_queries.py"
    ]
    
    missing_files = []
    
    for file_path in required_files:
        if not Path(file_path).exists():
            missing_files.append(file_path)
    
    if missing_files:
        print(f"❌ Missing files: {', '.join(missing_files)}")
        return False
    
    print("✓ New modules exist")
    return True


def test_cli():
    """Test if CLI works"""
    try:
        import subprocess
        result = subprocess.run([
            sys.executable, "-m", "src.cli", "--help"
        ], capture_output=True, text=True, timeout=10)
        
        if result.returncode == 0:
            print("✓ CLI is working")
            return True
        else:
            print(f"❌ CLI test failed: {result.stderr}")
            return False
    except Exception as e:
        print(f"❌ CLI test error: {e}")
        return False


def main():
    """Main setup function"""
    print("🚀 REPACSS Power Measurement Client - Enhanced Setup")
    print("=" * 60)
    
    # Check new structure
    print("\n📁 Checking new directory structure...")
    structure_ok = check_new_structure()
    
    # Check new modules
    print("\n📦 Checking new modules...")
    modules_ok = check_new_modules()
    
    # Check dependencies
    print("\n📦 Checking dependencies...")
    deps_ok = check_dependencies()
    
    # Create configuration file
    print("\n⚙️  Setting up configuration...")
    create_env_file()
    
    # Check gitignore
    print("\n🔒 Checking security...")
    gitignore_ok = check_gitignore()
    
    # Test CLI
    print("\n🧪 Testing CLI...")
    cli_ok = test_cli()
    
    print("\n" + "=" * 60)
    
    if all([structure_ok, modules_ok, deps_ok, gitignore_ok]):
        print("✅ Enhanced setup completed successfully!")
        print("\n📝 Next steps:")
        print("1. Edit .env with your database and SSH credentials")
        print("2. Test your connection: python -m src.cli test connection")
        print("3. Run power analysis: python -m src.cli analyze --database h100")
        print("4. Generate reports: python -m src.cli report excel")
        
        if cli_ok:
            print("\n🎉 CLI is working! You can now use:")
            print("  python -m src.cli --help")
            print("  python -m src.cli analyze --help")
            print("  python -m src.cli report --help")
            print("  python -m src.cli test --help")
    else:
        print("⚠️  Setup completed with issues")
        if not structure_ok:
            print("  - Missing directory structure")
        if not modules_ok:
            print("  - Missing new modules")
        if not deps_ok:
            print("  - Missing dependencies")
        if not gitignore_ok:
            print("  - Gitignore issues")
        if not cli_ok:
            print("  - CLI not working")
    
    print("\n📚 For more information, see README.md and USAGE_GUIDE.md")


if __name__ == "__main__":
    main()
