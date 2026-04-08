FROM reg.antgroup-inc.cn/antray/manylinux2014_x86_64:2022-12-20-b4884d9

RUN find /etc/yum.repos.d/ -type f ! -name "CentOS-Base.repo" -delete
RUN sed -i '/mirrorlist.centos.org/d' /etc/yum.repos.d/CentOS-Base.repo && \
    sed -i 's:mirror.centos.org:yum.tbsite.net:g' /etc/yum.repos.d/CentOS-Base.repo && \
    sed -i 's:^#baseurl:baseurl:g' /etc/yum.repos.d/CentOS-Base.repo

RUN yum install -y \
    gcc \
    make \
    perl \
    sudo \
    wget \
    less \
    zlib-devel \
    bzip2-devel \
    ncurses-devel \
    libffi-devel \
    readline-devel \
    openssl-devel \
    xz-devel \
    sqlite-devel \
    tk-devel \
    java-1.8.0-openjdk-devel \
    maven

RUN cd /tmp && \
    curl -O http://antsys-ray-prod.cn-shanghai-ant-office.oss-alipay.aliyuncs.com/moshi/openssl-1.1.1w.tar.gz && \
    tar -xzf openssl-1.1.1w.tar.gz && \
    cd openssl-1.1.1w && \
    ./config --prefix=/usr/local/openssl --openssldir=/usr/local/openssl shared zlib && \
    make && make install

RUN wget http://antsys-ray-prod.cn-shanghai-ant-office.oss-alipay.aliyuncs.com/moshi/bazel -O bazel && \
    chmod +x ./bazel && \
    mv ./bazel /usr/local/bin/

RUN echo "root:root" | chpasswd && \
    useradd -ms /bin/bash admin && \
    echo "admin:admin" | chpasswd && \
    usermod -aG wheel admin
USER admin
WORKDIR /home/admin

RUN wget http://antsys-ray-prod.cn-shanghai-ant-office.oss-alipay.aliyuncs.com/moshi/pyenv-2.3.31.zip && \
    unzip pyenv-2.3.31.zip && \
    rm pyenv-2.3.31.zip && \
    mv pyenv-2.3.31 ~/.pyenv && \
    echo 'export PYENV_ROOT="$HOME/.pyenv"' >> ~/.bashrc && \
    echo 'export PATH="$PYENV_ROOT/bin:$PYENV_ROOT/shims:$PATH"' >> ~/.bashrc && \
    echo 'eval "$(pyenv init --path)"' >> ~/.bashrc && \
    echo 'export OPENSSL_ROOT=/usr/local/openssl' >> ~/.bashrc && \
    echo 'export LD_LIBRARY_PATH="$OPENSSL_ROOT/lib:$LD_LIBRARY_PATH"' >> ~/.bashrc && \
    source ~/.bashrc && \
    mkdir -p ~/.pyenv/cache && \
    export CPPFLAGS="-I$OPENSSL_ROOT/include" && \
    export LDFLAGS="-L$OPENSSL_ROOT/lib" && \
    v=3.9.18 && \
    wget https://anpm.alibaba-inc.com/mirrors/python/$v/Python-$v.tar.xz -P ~/.pyenv/cache/ && \
    env PYTHON_CONFIGURE_OPTS="--enable-shared" pyenv install $v && \
    v=3.10.13 && \
    wget https://anpm.alibaba-inc.com/mirrors/python/$v/Python-$v.tar.xz -P ~/.pyenv/cache/ && \
    env PYTHON_CONFIGURE_OPTS="--enable-shared" pyenv install $v && \
    v=3.11.6 && \
    wget https://anpm.alibaba-inc.com/mirrors/python/$v/Python-$v.tar.xz -P ~/.pyenv/cache/ && \
    env PYTHON_CONFIGURE_OPTS="--enable-shared" pyenv install $v && \
    v=3.12.0 && \
    wget https://anpm.alibaba-inc.com/mirrors/python/$v/Python-$v.tar.xz -P ~/.pyenv/cache/ && \
    env PYTHON_CONFIGURE_OPTS="--enable-shared" pyenv install $v && \
    pyenv versions

RUN wget http://antsys-ray-prod.cn-shanghai-ant-office.oss-alipay.aliyuncs.com/moshi/nvm.tar.gz && \
    tar zxvf nvm.tar.gz && \
    rm nvm.tar.gz && \
    echo 'export PATH="/home/admin/.nvm/versions/node/v16.20.2/bin:$PATH"' >> ~/.bashrc

RUN echo "build --experimental_downloader_config=/home/admin/.bazel_downloader.cfg" >> /home/admin/.bazelrc
RUN echo "build --jobs=4" >> /home/admin/.bazelrc
RUN echo "rewrite github\.com/indygreg/python-build-standalone/releases/download/20220502/cpython-3\.9\.12\+20220502-x86_64-unknown-linux-gnu-install_only\.tar\.gz antsys-ray-prod.cn-shanghai-ant-office.oss-alipay.aliyuncs.com/moshi%2Findygreg%2Fpython-build-standalone%2Freleases%2Fdownload%2F20220502%2Fcpython-3.9.12%2B20220502-x86_64-unknown-linux-gnu-install_only.tar.gz" > /home/admin/.bazel_downloader.cfg && \
    echo "rewrite github\.com/indygreg/python-build-standalone/releases/download/20220502/cpython-3\.10\.4\+20220502-x86_64-unknown-linux-gnu-install_only\.tar\.gz antsys-ray-prod.cn-shanghai-ant-office.oss-alipay.aliyuncs.com/moshi%2Findygreg%2Fpython-build-standalone%2Freleases%2Fdownload%2F20220502%2Fcpython-3.10.4%2B20220502-x86_64-unknown-linux-gnu-install_only.tar.gz" >> /home/admin/.bazel_downloader.cfg && \
    echo "rewrite (github.com)/(.*) antsys-ray-prod.cn-shanghai-ant-office.oss-alipay.aliyuncs.com/moshi/\$2" >> /home/admin/.bazel_downloader.cfg && \
    echo "rewrite (files.pythonhosted.org)/(.*) antsys-ray-prod.cn-shanghai-ant-office.oss-alipay.aliyuncs.com/moshi/\$2" >> /home/admin/.bazel_downloader.cfg && \
    echo "rewrite (mirror.bazel.build)/(.*) antsys-ray-prod.cn-shanghai-ant-office.oss-alipay.aliyuncs.com/moshi/\$2" >> /home/admin/.bazel_downloader.cfg && \
    echo "rewrite (openssl.org)/(.*) antsys-ray-prod.cn-shanghai-ant-office.oss-alipay.aliyuncs.com/moshi/\$2" >> /home/admin/.bazel_downloader.cfg && \
    echo "rewrite (pilotfiber.dl.sourceforge.net)/(.*) antsys-ray-prod.cn-shanghai-ant-office.oss-alipay.aliyuncs.com/moshi/\$2" >> /home/admin/.bazel_downloader.cfg && \
    echo "rewrite golang\.org/dl/\?mode\=json\&include\=all antsys-ray-prod.cn-shanghai-ant-office.oss-alipay.aliyuncs.com/moshi/golang/golang_versions.json" >> /home/admin/.bazel_downloader.cfg && \
    echo "rewrite (repo1.maven.org)/maven2/(.*) artifacts.antgroup-inc.cn/artifact/repositories/maven-public/\$2" >> /home/admin/.bazel_downloader.cfg && \
    echo "allow antsys-ray-prod.cn-shanghai-ant-office.oss-alipay.aliyuncs.com" >> /home/admin/.bazel_downloader.cfg && \
    echo "allow artifacts.antgroup-inc.cn" >> /home/admin/.bazel_downloader.cfg && \
    echo "allow golang.org" >> /home/admin/.bazel_downloader.cfg && \
    echo "allow dl.google.com" >> /home/admin/.bazel_downloader.cfg && \
    echo "block *" >> /home/admin/.bazel_downloader.cfg

RUN echo "admin" | sudo -S ln -s /home/admin/.pyenv/shims/python3 /usr/bin/python3

CMD ["/bin/bash"]

RUN mkdir -p /home/admin/code
WORKDIR /home/admin/code
COPY --chown=admin:admin . /home/admin/code

RUN source ~/.bashrc && \
    cd python/ray/dashboard/client && \
    npm ci && \
    npm run build && \
    ls ./build

RUN source ~/.bashrc && \
    pyenv global 3.9.18 && \
    pip config set global.index-url https://pypi.antfin-inc.com/simple/ && \
    pip config set global.trusted-host pypi.antfin-inc.com && \
    pip install --upgrade setuptools wheel && \
    cd python/ && \
    RAY_INSTALL_JAVA=1 RAY_BUILD_REDIS=1 python setup.py bdist_wheel && \
    RAY_INSTALL_CPP=1 python setup.py bdist_wheel

RUN source ~/.bashrc && \
    pyenv global 3.10.13 && \
    pip config set global.index-url https://pypi.antfin-inc.com/simple/ && \
    pip config set global.trusted-host pypi.antfin-inc.com && \
    pip install --upgrade setuptools wheel && \
    cd python/ && \
    RAY_INSTALL_JAVA=1 RAY_BUILD_REDIS=1 python setup.py bdist_wheel && \
    RAY_INSTALL_CPP=1 python setup.py bdist_wheel

RUN source ~/.bashrc && \
    pyenv global 3.11.6 && \
    pip config set global.index-url https://pypi.antfin-inc.com/simple/ && \
    pip config set global.trusted-host pypi.antfin-inc.com && \
    pip install --upgrade setuptools wheel && \
    cd python/ && \
    RAY_INSTALL_JAVA=1 RAY_BUILD_REDIS=1 python setup.py bdist_wheel && \
    RAY_INSTALL_CPP=1 python setup.py bdist_wheel

RUN source ~/.bashrc && \
    pyenv global 3.12.0 && \
    pip config set global.index-url https://pypi.antfin-inc.com/simple/ && \
    pip config set global.trusted-host pypi.antfin-inc.com && \
    pip install --upgrade setuptools wheel && \
    cd python/ && \
    RAY_INSTALL_JAVA=1 RAY_BUILD_REDIS=1 python setup.py bdist_wheel && \
    RAY_INSTALL_CPP=1 python setup.py bdist_wheel

RUN source ~/.bashrc && \
    pip install auditwheel && \
    cd python/ && \
    auditwheel repair dist/ant_ray-*.whl -w wheelhouse/ && \
    cp dist/ant_ray_cpp-*.whl wheelhouse/
