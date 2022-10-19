# Docker image intended for local development using vscode

FROM ubuntu:latest

# [Optional] Uncomment this section to install additional OS packages.
RUN apt-get update && apt-get -y install unixodbc-dev \
    && apt-get -y install unixodbc \
    && apt-get -y install odbcinst \
    && apt-get -y install curl
COPY dameng_odbc_driver/ /usr/lib/dameng_odbc/
COPY odbcinst.ini /etc/odbcinst.ini
RUN echo "export LD_LIBRARY_PATH=/usr/lib/dameng_odbc:/usr/lib/x86_64-linux-gnu/:$LD_LIBRARY_PATH" >> ~/.bashrc
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --default-toolchain stable -y