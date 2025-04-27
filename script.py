import requests
import json
import time
from supabase import create_client, Client

# === Supabase Configuration ===
SUPABASE_URL = "https://bexyddvivmvrcshbrgdr.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJleHlkZHZpdm12cmNzaGJyZ2RyIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDQ5MjQ2MDUsImV4cCI6MjA2MDUwMDYwNX0.fgo625QbC-2_hzlXDj2J-Xerjo9T-vEaudl2zgIwJbg"
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# === LeetCode GraphQL ===
GRAPHQL_URL = "https://leetcode.com/graphql/"
HEADERS = {"Content-Type": "application/json"}

QUERY_PROBLEM_LIST = """
query problemsetQuestionList($categorySlug: String, $limit: Int, $skip: Int, $filters: QuestionListFilterInput) {
  problemsetQuestionList: questionList(
    categorySlug: $categorySlug,
    limit: $limit,
    skip: $skip,
    filters: $filters
  ) {
    total: totalNum
    questions: data {
      acRate
      difficulty
      freqBar
      isPaidOnly
      title
      titleSlug
      topicTags {
        name
        id
        slug
      }
    }
  }
}
"""

QUERY_QUESTION_CONTENT = """
query questionContent($titleSlug: String!) {
  question(titleSlug: $titleSlug) {
    content
    mysqlSchemas
  }
}
"""

def fetch_problem_list(limit=50):
    variables = {
        "categorySlug": "",
        "skip": 0,
        "limit": limit,
        "filters": {}
    }
    response = requests.post(GRAPHQL_URL, headers=HEADERS, json={
        "query": QUERY_PROBLEM_LIST,
        "variables": variables
    })
    response.raise_for_status()
    return response.json()["data"]["problemsetQuestionList"]["questions"]

def fetch_question_content(slug):
    response = requests.post(GRAPHQL_URL, headers=HEADERS, json={
        "query": QUERY_QUESTION_CONTENT,
        "variables": { "titleSlug": slug }
    })
    response.raise_for_status()
    return response.json()["data"]["question"]

def insert_topic_tags(tags):
    slug_to_id = {}
    for tag in tags:
        try:
            # Try to insert the tag
            response = supabase.table("topic_tags").insert({
                "name": tag["name"],
                "slug": tag["slug"]
            }).execute()

            # If inserted successfully, capture the new ID
            if response.data and isinstance(response.data, list):
                tag_id = response.data[0]["id"]
                slug_to_id[tag["slug"]] = tag_id

        except Exception as e:
            # Conflict error — tag already exists
            if "duplicate key" in str(e) or "23505" in str(e):
                # Fetch the existing tag's ID
                lookup = supabase.table("topic_tags").select("id").eq("slug", tag["slug"]).limit(1).execute()
                if lookup.data:
                    slug_to_id[tag["slug"]] = lookup.data[0]["id"]
            else:
                raise e  # unexpected error

    return slug_to_id

def insert_question_and_relations(q):
    # Insert question (ID auto-generated)
    question_data = {
        "title": q["title"],
        "title_slug": q["titleSlug"],
        "difficulty": q.get("difficulty"),
        "ac_rate": q.get("acRate"),
        "freq_bar": q.get("freqBar"),
        "is_paid_only": q.get("isPaidOnly", False),
        "content": q.get("content")
    }

    response = supabase.table("questions").insert(question_data).execute()
    if not response.data or not isinstance(response.data, list):
        raise Exception(f"Insert failed: {response}")
    question_id = response.data[0]["id"]

    # Upsert tags and fetch tag IDs
    slug_to_id = insert_topic_tags(q["topicTags"])

    # Insert join records
    for tag in q["topicTags"]:
        tag_id = slug_to_id.get(tag["slug"])
        if tag_id:
            supabase.table("question_topic_tags").insert({
                "question_id": question_id,
                "topic_tag_id": tag_id
            }).execute()

def main():
    print("🚀 Fetching problem list...")
    try:
        questions = fetch_problem_list(limit=50)  # change to 100 or paginate if needed
    except Exception as e:
        print("❌ Failed to fetch problem list:", e)
        return

    for idx, q in enumerate(questions):
        print(f"[{idx+1}/{len(questions)}] {q['titleSlug']}")
        try:
            # Fetch full content & schemas
            content_data = fetch_question_content(q["titleSlug"])
            q["content"] = content_data.get("content")
            # Insert into DB
            insert_question_and_relations(q)
        except Exception as e:
            print(f"  ⚠️ Error processing {q['titleSlug']}: {e}")
        time.sleep(0.5)

    print("✅ Done uploading to Supabase!")

if __name__ == "__main__":
    main()
