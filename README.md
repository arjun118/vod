# implementation details

1. the authentication is only required when uploading a video and getting the playback url
2. once the playback url is received - nginx will forward the request to minio
3. the bucket is public here. so everyone with a playback url can access this
4. authentication doesnot apply to playback in this one (a bit odd but yeah ik)

- everything will be the same as above
- we add a cache at the reverse proxy level (nginx)
# cache notes

```nginx
proxy_cache_path /var/cache/nginx/hls_cache 
    levels=1:2 
    keys_zone=hls_cache_zone:20m 
    max_size=50g 
    inactive=24h 
    use_temp_path=off;
```

> proxy_cache_path: sets the path and configuration of the cache
> proxy_cache activates it

  - `level`: 2 level directory heirarchy under /var/cache/nginx/hls_cache (cache path)
  - eg: key hash: ab1234bc............
  - directory structure: /var/cache/nginx/hls_cache/a/b1/2345bc........
  - `keys_zone`: shared memory zone for storing the cache keys and metadata such as usage timers
  - copy of keys in memory: enabled nginx to quickly determine if a request is a `HIT` or `MISS` without having to go to the disk
  - 1-MB store , can store upto 8000 keys
  - `max_size`:  upperlimit of the size of the cache - when full `cache manager` removes the files that were least recently used
  - `inactive`: how long an item can remain in the cache without being accessed (without being accessed) 
  - `inactive content != expired content`
  - expired (stale) content is deleted only when it has not been accessed for the time  specified by `inactive`
  - nginx resets the inactive timer when expired content is accessed
  - `if nobody access an item for **inactive** minutes, it will be deteled from the disk (cache) - even if it hasn't expired`
  - `use_temp_path`: instructs nginx to write them to same directories where they  will be cached. otherwise nginx first writes files that are destined for the cache to a temporary storage area.
  - `proxy_cache_use_stale`: used inside `location` - deliver stale cached content when it cant get fresh content from origin servers. provides extra level of fault tolerance for the servers and ensure uptime incase of server failures and traffic spikes
  - ```nginx
     location / {
      # ...
      proxy_cache_use_stale error timeout http_500 http_502 http_503 http_504;
    }``` 
  - With this sample configuration, if NGINX receives an error, timeout, or any of the specified 5xx errors from the origin server and it has a stale version of the requested file in its cache, it delivers the stale file instead of relaying the error to the client.

  1. `Cache-Control`: about freshness
  2. `Inactive`: Garbage collection

## Scenarios

1. if the `file` becomes inactive even though it still did not expire (`Cache-Control` by origin server), `nginx` may remove it 
2. if the `file` expires before becoming inactive, and the next request from the client requests this file, 
  - nginx checks cache: `result -> expired`
  - nginx sends: 
    - GET /video
      If None-Match: abc123
    to origin
  - if origin responds: `304 not modified`, nginx refres cache metadata and keep existing body
  - if origin responds: `200 Ok New content`, replace cached object

```nginx
server {
    # ...
    location / {
        proxy_cache my_cache;
        proxy_cache_revalidate on;
        proxy_cache_min_uses 3;
        proxy_cache_use_stale error timeout updating http_500 http_502
                            http_503 http_504;
        proxy_cache_background_update on;
        proxy_cache_lock on;

        proxy_pass http://my_upstream;
    }
}
```

- `proxy_cache_revalidate`: use conditional get requests when refreshing content from the origin servers. 
- If a client requests an item that is cached but expired as defined by the cache control headers, NGINX includes the `If-Modified-Since` field in the header of the GET request it sends to the origin server. This saves on bandwidth, because the server sends the full item only if it has been modified since the time recorded in the `Last-Modified` header attached to the file when NGINX originally cached it.
- `proxy_cache_min_uses` sets the number of times an item must be requested by clients before NGINX caches it. This is useful if the cache is constantly filling up, as it ensures that only the most frequently accessed items are added to the cache. By default proxy_cache_min_uses is set to 1.
- The updating parameter to the `proxy_cache_use_stale` directive, combined with enabling the `proxy_cache_background_update` directive, instructs NGINX to deliver stale content when clients request an item that is expired or is in the process of being updated from the origin server. All updates will be done in the background. The stale file is returned for all requests until the updated file is fully downloaded.
- With `proxy_cache_lock` enabled, if multiple clients request a file that is not current in the cache (a MISS), only the first of those requests is allowed through to the origin server. The remaining requests wait for that request to be satisfied and then pull the file from the cache. Without proxy_cache_lock enabled, all requests that result in cache misses go straight to the origin server.

## instrumenting nginx cache

> add_header X-Cache-Status $upstream_cache_status;

This example adds an X-Cache-Status HTTP header in responses to clients. The following are the possible values for $upstream_cache_status:

- `MISS` – The response was not found in the cache and so was fetched from an origin server. The response might then have been cached.
- `BYPASS` – The response was fetched from the origin server instead of served from the cache because the request matched a proxy_cache_bypass directive (see Can I Punch a Hole Through My Cache? below.) The response might then have been cached.
- `EXPIRED` – The entry in the cache has expired. The response contains fresh content from the origin server.
- `STALE` – The content is stale because the origin server is not responding correctly, and proxy_cache_use_stale was configured.
- `UPDATING` – The content is stale because the entry is currently being updated in response to a previous request, and proxy_cache_use_stale updating is configured.
- `REVALIDATED` – The proxy_cache_revalidate directive was enabled and NGINX verified that the current cached content was still valid (If-Modified-Since or If-None-Match).
- `HIT` – The response contains valid, fresh content direct from the cache.

## cache or not to cache

- cache a response only if the origin server includes either the `Expires` header with a date and a time in future or the Cache-Control header with the max-age directive set to a non-zero value
- it `only caches responses to GET and HEAD requests`


## how to reason and implement

### If-None-Match , If-Modified-Since, Last-Modified , ETag are manually handled 


### Cache-Control comes from origin

> Cache-Control: max-age=3600

## production usage (for static where you know better than the application)

```nginx
proxy_ignore_headers Cache-Control Expires;

proxy_cache_valid 200 24h;
proxy_cache_valid 206 24h;
```
because
```
playlist.m3u8
segment.ts
```
rarely change

> in this scenario cache


## adding cache hit or miss logs

```nginx
log_format cachelog
'$remote_addr '
'$request '
'$status '
'$upstream_cache_status';

access_log /var/log/nginx/cache.log cachelog;
```

## timers and behaviours involved

1. proxy_cache_valid
2. proxy_cache_revalidate
3. inactive

I have

> proxy_cache_valid

I do not have

> proxy_cache_revalidate

## flow

### 1.first request to file

1. cache miss
2. fetch from origin server
3. cache entry created

#### next 24 hrs

any request for this same file will be - `cache HIT`

#### after 24 hrs

1. cache entry expires
2. next request arrives for the same file

> if proxy_cache_revalidate is `OFF`

- THE ENTIRE OBJECT WILL BE REDOWNLOADED - `NO CONDITIONAL GET REQUEST WILL BE SENT TO MINIO`
```
Expired
   |
   v
GET object again
   |
   v
Replace cache entry
```
### if proxy_cache_revalidate: on  ?


now after 24 hrs , when the file is accesed again

nginx sends to minio

 ```bash
GET /storage/foo.ts

If-None-Match: abc123
 ```

>  minio response: 304 Not Modified

> nginx: keep exisiting file, refresh metadata


----

Everything remains the same from the previous method - nginx cache method. the changes are below

## change in nginx config

1. we add an internal vritual host `/_protected/` and direct our `/media/` route to our go backed for authentication

```nginx
 location /media/{
        # this will be a validating/auth backend now
        # responds with x-accel-redirect header for nginx to know
        # that the request is valid and it can serve the file
            proxy_pass http://backend:8080;
            proxy_set_header Host $host;
            proxy_set_header X-Real-IP $remote_addr;
        }
```

```nginx
location /_protected/ {

            internal;

            proxy_cache hls_cache;

            proxy_cache_key "$scheme$proxy_host$uri";

            proxy_ignore_headers Cache-Control Expires Set-Cookie;

            proxy_cache_valid 200 24h;
            proxy_cache_valid 206 24h;

            proxy_cache_use_stale
                error
                timeout
                http_500
                http_502
                http_503
                http_504;

            proxy_cache_background_update on;
            proxy_cache_lock on;
            proxy_cache_lock_timeout 10s;

            proxy_force_ranges on;

            add_header X-Cache-Status $upstream_cache_status always;
            add_header X-Debug-Uri $uri always;


            proxy_pass http://minio:9000/;
        }
```

## code changes

1. we seperate our token authentication - which our general access and cookie authentication - specifically to validate media access 
2. `/api/videos/auth-cookie` - just returns a cookie now, in real use cases - you might want to check the user's permissions to view the video and return a token or deny access

## media url ==  exact object key?

1. yes, for now. since we dont have any database stuff going on here to map `media/videos/123` to `storage/videos/26/06/object_key`. if you integrate some db , you can easily map and rewrite the `X-Accel-Redirect` path to your object storage url.
2. this time we also hid our bucket name in the request - since we generate the path that nginx needs to fetch


----

# TODO: will add a flow diagram on how the request lifecycle
