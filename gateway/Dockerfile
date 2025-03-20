FROM caddy:alpine
ENV PORT=8080
EXPOSE 8080

COPY --from=ghcr.io/fivexl/lprobe:0.1.5 /lprobe /bin/lprobe
COPY ./index.html /usr/share/caddy/index.html
COPY ./Caddyfile /etc/caddy/Caddyfile
RUN apk add --no-cache amazon-ssm-agent && \
    rc-update add amazon-ssm-agent default && \
    service amazon-ssm-agent start

VOLUME /tmp
ADD --chmod=644 https://www.vexforum.com/uploads/default/original/3X/7/5/75c8d5397f6e2dab39b00d73c3f7e5b2cec4f616.jpeg /usr/share/caddy/init.jpg
