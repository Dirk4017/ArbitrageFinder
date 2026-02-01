import os
import sys
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


def test_supabase():
    """Test Supabase connection"""
    print("🧪 Testing Supabase connection...")

    supabase_url = os.getenv('SUPABASE_URL')
    supabase_key = os.getenv('SUPABASE_KEY')

    print(f"Supabase URL: {'✅ Present' if supabase_url else '❌ Missing'}")
    print(f"Supabase Key: {'✅ Present' if supabase_key else '❌ Missing'}")

    if not supabase_url or not supabase_key:
        print("❌ Missing Supabase credentials!")
        return False

    try:
        # Test import
        from supabase import create_client

        # Create client
        print("🔌 Creating Supabase client...")
        supabase = create_client(supabase_url, supabase_key)

        # Test query
        print("📊 Testing query...")
        response = supabase.table('bets').select('count', count='exact').execute()

        print(f"✅ Success! Table 'bets' has {response.count} rows")
        return True

    except ImportError as e:
        print(f"❌ Failed to import supabase: {e}")
        return False
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = test_supabase()
    sys.exit(0 if success else 1)