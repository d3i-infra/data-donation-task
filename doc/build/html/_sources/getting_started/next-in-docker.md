# Try out Next with Docker

This tutorial outlines how you can run Next in a Docker container. 

This is great for trying out the Next platform and will show you the necessary settings you need to use in production.

This tutorial is for advanced users.

## Prerequisites

In order for you to try out Next you need to set up prerequisites.

### Unsplash

Configure a developer account at [unsplash](https://unsplash.com/) and get an API key. You can do this for free. 

Unsplash is used as the source for banner images used to customize studies.


### Google OIDC

Configure a google OIDC connect application in the [google cloud console](https://console.cloud.google.com/welcome?project=stalwart-yen-241815). For the details check the [official instructions](https://developers.google.com/identity/openid-connect/openid-connect).

Google OIDC (OpenID Connect) is used to manage user authentication and account sign-ins.
If you are following this example you should configure `https://localhost:443/google-sign-in/auth` as redirect url


## Run Next in a Docker container

In this step, we will create and run the necessary containers using Docker Compose.

We are going to create a folder with the following structure:

```
.
├── db
│   ├── ca.crt
│   ├── server.crt
│   └── server.key
├── docker-compose.yml
└── proxy
    ├── certs
    │   ├── nginx-selfsigned.crt
    │   └── nginx-selfsigned.key
    └── conf
        └── nginx.conf
```

In the next step we are going to create the files.


### Build the Next Docker image

Clone or fork [Next](https://github.com/eyra/mono)

`cd` into `./mono/core`

and build the image with:

```
docker build  --build-arg VERSION=1.0.0 --build-arg BUNDLE=self . -t self-d3i:latest
```

### Setup certificates for TLS

Create certificates and put them in `proxy/certs`

Use the following command:

```
openssl req -x509 -nodes -days 365 -newkey rsa:2048 -keyout nginx-selfsigned.key -out nginx-selfsigned.crt
```

### Nginx configuration

We are going to use Nginx as [reverse proxy](https://docs.nginx.com/nginx/admin-guide/web-server/reverse-proxy/).

Nginx will be used to provide TLS for our HTTP connections.

Paste the following nginx configuration in `proxy/conf`:

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

This Nginx configuration works with websocket connections which Next (Phoenix web application) uses.

### Setup certificates for Postgres

Next uses TLS to communicate with Posgres. For this you need to setup certificates.

`cd` into `./mono/postgres_ssl` and run `./generate.sh`. Place  `ca.crt`, `server.crt`, and `server.key` in the `db` folder you have just created.



### Docker compose yaml

Now create the docker-compose.yaml: 

```
#docker-compose.yaml
services:
  app:
    image: self-d3i:latest
    container_name: self-d3i
    restart: always
    environment:
      DB_TLS_VERIFY: verify_peer
      DB_CA_PATH: /ca.crt
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
      STORAGE_SERVICES: "builtin"
    volumes:
      - app_data:/tmp
      - ./db/ca.crt:/ca.crt
    depends_on:
      - db

  db:
    image: postgres:15
    container_name: db-next
    restart: always
    user: root
    environment:
      POSTGRES_USER: user
      POSTGRES_PASSWORD: password
      POSTGRES_DB: test_database
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./db/server.crt:/var/lib/postgresql/ssl/server.crt
      - ./db/server.key:/var/lib/postgresql/ssl/server.key
    command: |
      bash -c "
        chown postgres:postgres /var/lib/postgresql/ssl/server.key &&
        exec docker-entrypoint.sh postgres -c ssl=on -c ssl_cert_file=/var/lib/postgresql/ssl/server.crt -c ssl_key_file=/var/lib/postgresql/ssl/server.key
      "

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

and replace the following variables with the values you obtained in the previous steps:

```
UNSPLASH_ACCESS_KEY: "<your-unsplash-api-key>"
UNSPLASH_APP_NAME: "<your-unsplash-app-name>"
GOOGLE_SIGN_IN_CLIENT_ID: "<your-google-oidc-client-id>"
GOOGLE_SIGN_IN_CLIENT_SECRET: "<your-google-oidc-client-secret>"
```

If you want to learn more about the variables you can read the [documentation](https://github.com/eyra/mono/blob/master/SELFHOSTING.md).

Now you are ready to start the containers with the following command:

```
docker compose up
```

Go to `https://localhost:80` and if everything went well you should see Next.

Note: because you self-signed your TLS certificates your browser will complain: accept all the risks and continue.

## Next steps in Next

Now you can play around in Next: to login as admin go to `/admin/login`.
