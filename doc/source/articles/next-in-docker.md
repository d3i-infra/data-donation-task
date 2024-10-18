# Tutorial try out Next yourself with Docker

This tutorial outlines how you can run Next in a docker container. 
This is great for trying out the software and will show you the necessary settings to run in production, if you want to run your own data donation study this way.

This tutorial:




## Build Next Docker image

Clone or fork [Next](https://github.com/eyra/mono)

`cd` into `/core`

and build the image with:

```
docker build  --build-arg VERSION=1.0.0 --build-arg BUNDLE=self . -t self-d3i:latest
```

## prerequisite

### Unsplash

Configure a developer account at [unsplash](https://unsplash.com/) and get an API key, you can do this for free. 

### Google OIDC

Configure a google OIDC connect application in the [google cloud console](https://console.cloud.google.com/welcome?project=stalwart-yen-241815).
For the details checkthe [official instructions](https://developers.google.com/identity/openid-connect/openid-connect).

### Certificates for TLS

Create certificates and put them in `proxy/certs`

```
openssl req -x509 -nodes -days 365 -newkey rsa:2048 -keyout nginx-selfsigned.key -out nginx-selfsigned.crt
```

### Configuration files

Directory structure

```
.
├── docker-compose.yaml
├── proxy
│   ├── certs
│   │   ├── nginx-selfsigned.crt
│   │   └── nginx-selfsigned.key
│   └── conf
│       └── nginx.conf
```

See these [instructions](https://github.com/eyra/mono/blob/master/SELFHOSTING.md) if you want to learn more about the environmental variables.


```
#docker-compose.yaml
services:
  app:
    image: self-d3i:latest
    container_name: self-d3i
    restart: always
    environment:
      APP_NAME: next
      APP_DOMAIN: localhost
      APP_MAIL_DOMAIN: "@gmail"
      APP_ADMINS: youremail@gmail.com
      DB_USER: user
      DB_PASS: password
      DB_HOST: db
      DB_NAME: test_database
      SECRET_KEY_BASE: "aUMZobj7oJn58XIlMGVcwTYrCsAllwDCGlwDCGlwDCGwDChdhsjahdghaggdgdGt7MoQYJtJbA="
      STATIC_PATH: "/tmp"
      UNSPLASH_ACCESS_KEY: "<your-unsplash-api-key>"
      UNSPLASH_APP_NAME: "<your-unsplash-app-name>"
      GOOGLE_SIGN_IN_CLIENT_ID: "<your-google-oidc-client-id>"
      GOOGLE_SIGN_IN_CLIENT_SECRET: "<your-google-oidc-client-secret>"
      STORAGE_SERVICES: "builtin, yoda, azure"
    volumes:
      - app_data:/tmp
    depends_on:
      - db

  db:
    image: postgres:latest
    container_name: db-next
    restart: always
    environment:
      POSTGRES_USER: user
      POSTGRES_PASSWORD: password
      POSTGRES_DB: test_database
    volumes:
      - postgres_data:/var/lib/postgresql/data

  proxy:
    image: nginx:latest
    container_name: nginx
    ports:
      - 443:443
    volumes:
      - ./proxy/conf/nginx.conf:/etc/nginx/nginx.conf
      - ./proxy/certs:/etc/nginx/certs
    depends_on:
      - app

volumes:
  postgres_data:
  app_data:
```

```
# nginx.conf
events {}
http {
    server {
        listen 80;
        listen [::]:80;
        server_name localhost;
        
        # Redirect all HTTP requests to HTTPS
        return 301 https://$server_name$request_uri;
    }

    server {
        server_name localhost;
        
        if ($scheme != "https") {
            return 301 https://$host$request_uri;
        }
        
        location / {
          allow all;
          proxy_pass                http://app:8000;
          proxy_set_header          X-Forwarded-Proto $scheme;
          proxy_set_header          X-Forwarded-For $remote_addr;
          proxy_set_header          X-Real-IP $remote_addr;
          proxy_set_header          Host $http_host;
          proxy_http_version        1.1;
          proxy_set_header          Upgrade $http_upgrade;
          proxy_set_header          Connection "upgrade";
          proxy_max_temp_file_size  1m;
        }
        
        listen 443 ssl;
        ssl_certificate /etc/nginx/certs/nginx-selfsigned.crt;
        ssl_certificate_key /etc/nginx/certs/nginx-selfsigned.key;
    }
}
```

