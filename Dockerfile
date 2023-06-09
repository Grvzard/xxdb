FROM python:3.10-slim-buster


# RUN apt-get update && \
#     apt-get install git -y
# RUN pip3 install xxdb[server]@git+https://github.com/Grvzard/xxdb@main

COPY xxdb /app/xxdb/
COPY pyproject.toml /app/
RUN pip3 install -e /app[server]

WORKDIR /app/data

VOLUME /app/data

EXPOSE 7791

ENTRYPOINT ["xxdb", "serve"]
CMD ["--config", "xxdb.toml"]
