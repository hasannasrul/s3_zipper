import io
import zipfile
from src.s3_zipper.zip_flow import generate_zip_stream, upload_stream_to_s3
from src.s3_zipper.progress import ProgressTracker


def test_zip_stream_and_upload(s3):
    source_bucket = "source-bucket"
    target_bucket = "target-bucket"
    prefix = "data/"

    s3.create_bucket(Bucket=source_bucket)
    s3.create_bucket(Bucket=target_bucket)

    s3.put_object(Bucket=source_bucket, Key="data/a.txt", Body=b"AAA")
    s3.put_object(Bucket=source_bucket, Key="data/b.txt", Body=b"BBB")

    # progress is required by real function
    progress = ProgressTracker(total_bytes=6, label="TEST")

    zip_stream = generate_zip_stream(s3, source_bucket, prefix, progress)

    upload_stream_to_s3(
        target_client=s3,
        bucket=target_bucket,
        key="out.zip",
        stream=zip_stream,
    )

    # Validate zip uploaded
    obj = s3.get_object(Bucket=target_bucket, Key="out.zip")
    data = obj["Body"].read()

    with zipfile.ZipFile(io.BytesIO(data), "r") as z:
        assert sorted(z.namelist()) == ["a.txt", "b.txt"]
        assert z.read("a.txt") == b"AAA"
        assert z.read("b.txt") == b"BBB"
