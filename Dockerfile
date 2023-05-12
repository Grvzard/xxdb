FROM python:3.10-slim-buster


# RUN apt-get update && \
#     apt-get install git -y
# RUN pip3 install xxdb[server]@git+https://github.com/Grvzard/xxdb@main

COPY xxdb /app/xxdb/
COPY pyproject.toml /app/xxdb/
RUN pip3 install -e /app/xxdb/[server]

WORKDIR /app/data
VOLUME /app/data

EXPOSE 7791

ENTRYPOINT ["xxdb"]
CMD ["--config xxdb.toml"]
