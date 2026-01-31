import os
from supabase import create_client


def test_connection():
    """Test Supabase connection"""
    print("🧪 Testing GitHub Actions setup...")

    # Get credentials from environment
    supabase_url = os.getenv('SUPABASE_URL')
    supabase_key = os.getenv('SUPABASE_KEY')

    if not supabase_url or not supabase_key:
        print("❌ Missing Supabase credentials!")
        print(f"URL present: {'Yes' if supabase_url else 'No'}")
        print(f"Key present: {'Yes' if supabase_key else 'No'}")
        return False

    try:
        # Test connection
        supabase = create_client(supabase_url, supabase_key)

        # Simple query to test
        response = supabase.table('bets').select('*').limit(1).execute()

        print(f"✅ Connected to Supabase successfully!")
        print(f"   Found {len(response.data)} bets in database")
        return True

    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False


if __name__ == "__main__":
    test_connection()