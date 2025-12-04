import io
import zipfile
from src.s3_zipper.unzip_flow import stream_zip_entries, multipart_upload_stream


def test_stream_zip_entries_and_upload(s3):
    bucket = "bucket"
    s3.create_bucket(Bucket=bucket)

    # Build ZIP in memory
    mem_zip = io.BytesIO()
    with zipfile.ZipFile(mem_zip, "w") as z:
        z.writestr("hello.txt", b"HELLO")
        z.writestr("world.txt", b"WORLD")
    mem_zip.seek(0)

    s3.put_object(Bucket=bucket, Key="test.zip", Body=mem_zip.getvalue())

    # DO NOT turn into a list (this closes the file early!)
    count = 0

    for name, generator in stream_zip_entries(s3, bucket, "test.zip"):
        count += 1

        # Consume generator NOW (while zip entry is still open)
        full_bytes = b"".join(list(generator))

        # Upload using multipart
        multipart_upload_stream(
            target_client=s3,
            bucket=bucket,
            key=f"unzipped/{name}",
            stream=[full_bytes],  # stream of 1 chunk; OK
        )

        # Validate upload
        obj = s3.get_object(Bucket=bucket, Key=f"unzipped/{name}")
        assert obj["Body"].read() == full_bytes

    assert count == 2
