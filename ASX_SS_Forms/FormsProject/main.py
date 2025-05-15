from s3_manager import S3Manager
from data_utils import find_s3_docs_by_refs

# if __name__ == "__main__":
#     s3 = S3Manager()
#     bucket_name = "xtf-data"
#     ticker = "MQG"

#     # Just list matching documents
#     matched_files = find_s3_docs_by_refs(ticker, s3, bucket_name, download=False)
#     print(matched_files)

#     # To download:
#     # matched_files = find_s3_docs_by_refs(ticker, s3, bucket_name, download=True)


if __name__ == "__main__":
    # Initialize S3 manager and correct bucket name
    s3 = S3Manager()
    bucket_name = "xtf-asx"
    ticker = "MQG"

    # List a few sample S3 keys to verify structure
    # print("\n🔍 Sample S3 keys in bucket:")
    # keys = s3.list_s3_objects(bucket_name)
    # for k in keys[:20]:
    #     print("-", k)

    # print(keys)

    # Find document refs in S3 that match the SQL refs for the ticker
    print(f"\n🔍 Now trying to match documents for ticker: {ticker}")
    matched_files = find_s3_docs_by_refs(ticker, s3, bucket_name, download=False)

    print("\n✅ Matched S3 files:")
    print(matched_files)
