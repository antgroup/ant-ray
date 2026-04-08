FROM reg.antgroup-inc.cn/antray/ant-ray-ci:79304b0ec7b1b078fc255d29373a06f6b383df96

USER admin
WORKDIR /home/admin

RUN wget http://antsys-ray-prod.cn-shanghai-ant-office.oss-alipay.aliyuncs.com/moshi/bazel -O bazel && \
    chmod +x ./bazel && \
    mv ./bazel /home/admin/bin/bazel

RUN source /home/admin/.bashrc && pyenv global 3.10.13 && python --version

COPY --chown=admin:admin . .

RUN echo "build --experimental_downloader_config=/home/admin/.bazel_downloader.cfg" >> /home/admin/.bazelrc && \
    echo "build --jobs=6" >> /home/admin/.bazelrc
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
RUN echo "export JAVA_HOME=/opt/taobao/java" >> /home/admin/.bashrc && \
    echo 'export PATH=$JAVA_HOME/bin:$PATH' >> /home/admin/.bashrc

RUN source /home/admin/.bashrc && /home/admin/bin/bazel build //:gen_ray_pkg //cpp:gen_ray_cpp_pkg //java:gen_ray_java_pkg //:gen_redis_pkg \
    //java:gen_maven_deps //java:all_tests_shaded.jar \
    --remote_executor=grpc://11.165.249.241:8980 --jobs=60

RUN cd python/ray/dashboard/client && \
    wget http://antsys-ray-prod.cn-shanghai-ant-office.oss-alipay.aliyuncs.com/moshi/nvm.tar.gz && \
    tar zxvf nvm.tar.gz && \
    mv .nvm/ /home/admin/.nvm/ && \
    export PATH="/home/admin/.nvm/versions/node/v16.20.2/bin:$PATH" && \
    npm ci && \
    npm run build && \
    ls ./build

RUN source /home/admin/.bashrc && \
    pyenv global 3.10.13 && \
    cd python/ && RAY_INSTALL_JAVA=1 RAY_BUILD_REDIS=1 python setup.py bdist_wheel && \
    pip install "$(find dist -name "ant_ray-*.whl")"[default] && \
    RAY_INSTALL_CPP=1 python setup.py bdist_wheel && \
    pip install "$(find dist -name "ant_ray_cpp-*.whl")" && \
    pip list

CMD ["/bin/bash"]
