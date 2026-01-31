"""
Setup script for the Sports Betting System
"""
import os
import sys
import subprocess


def check_requirements():
    """Check if all required packages are installed"""
    required_packages = [
        'pandas',
        'numpy',
        'requests',
        'selenium',
        'fuzzywuzzy',
        'python-Levenshtein',  # For faster fuzzy matching
    ]

    print("Checking requirements...")

    for package in required_packages:
        try:
            __import__(package.replace('-', '_'))
            print(f"✓ {package}")
        except ImportError:
            print(f"✗ {package} - installing...")
            subprocess.check_call([sys.executable, "-m", "pip", "install", package])

    # Check for R
    try:
        subprocess.run(["Rscript", "--version"], capture_output=True, check=True)
        print("✓ R installed")
    except:
        print("✗ R not found. Please install R from https://cran.r-project.org/")
        print("  Required R packages: nflreadr, hoopR, dplyr, jsonlite, stringr, lubridate")

    print("\nSetup complete!")


def create_directory_structure():
    """Create the directory structure"""
    directories = [
        'core',
        'resolvers',
        'scraper',
        'database',
        'arbitrage',
        'utils'
    ]

    for directory in directories:
        os.makedirs(directory, exist_ok=True)
        print(f"Created directory: {directory}/")

    # Create empty __init__.py files
    for directory in directories:
        init_file = os.path.join(directory, '__init__.py')
        with open(init_file, 'w') as f:
            f.write(f"# {directory} module\n")
        print(f"Created: {init_file}")


def main():
    """Main setup function"""
    print("=" * 60)
    print("SPORTS BETTING SYSTEM SETUP")
    print("=" * 60)

    # Create directory structure
    print("\n1. Creating directory structure...")
    create_directory_structure()

    # Check requirements
    print("\n2. Checking requirements...")
    check_requirements()

    print("\n" + "=" * 60)
    print("SETUP COMPLETE!")
    print("=" * 60)
    print("\nNext steps:")
    print("1. Edit config.json with your settings")
    print("2. Run: python main.py")
    print("3. Test the system with demo data first")


if __name__ == "__main__":
    main()