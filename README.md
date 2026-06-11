# implementation details

1. the authentication is only required when uploading a video and getting the playback url
2. once the playback url is received - nginx will forward the request to minio
3. the bucket is public here. so everyone with a playback url can access this
4. authentication doesnot apply to playback in this one (a bit odd but yeah ik)
