FROM giovtorres/docker-centos7-slurm:latest

# 1) Point CentOS 7 repos to vault (EOL-safe)
RUN set -eux; \
  for f in /etc/yum.repos.d/CentOS-*.repo; do \
    sed -ri 's|^mirrorlist=.*|# &|g' "$f" || true; \
    sed -ri 's|^#?baseurl=.*centos/\$releasever|baseurl=http://vault.centos.org/7.9.2009|g' "$f" || true; \
  done; \
  yum clean all

# 2) Remove stale 3rd-party repos (IUS/EPEL) from base image
RUN rm -f /etc/yum.repos.d/ius*.repo /etc/yum.repos.d/epel*.repo || true && \
    yum clean all

# 3) Add SCLo repos (devtoolset-3, rh-python38) with GPG disabled (vault key mismatch)
RUN cat > /etc/yum.repos.d/CentOS-SCLo.repo <<'EOF'
[centos-sclo-rh]
name=CentOS-7 - SCLo rh
baseurl=http://vault.centos.org/7.9.2009/sclo/$basearch/rh/
gpgcheck=0
enabled=1

[centos-sclo-sclo]
name=CentOS-7 - SCLo sclo
baseurl=http://vault.centos.org/7.9.2009/sclo/$basearch/sclo/
gpgcheck=0
enabled=1
EOF

# 4) Core packages (base/updates/extras + SCLo only)
RUN yum -y --nogpgcheck --disablerepo="*" --enablerepo=base,updates,extras,centos-sclo-* install \
      environment-modules tcl \
      openssh-server openssh-clients \
      which git wget curl tar unzip bzip2 xz \
      gcc gcc-c++ make autoconf automake libtool \
      zlib-devel bzip2-devel xz-devel \
      ncurses-devel readline-devel openssl-devel \
      perl-ExtUtils-MakeMaker \
      devtoolset-3 devtoolset-3-runtime \
      devtoolset-3-gcc devtoolset-3-gcc-c++ devtoolset-3-binutils \
      rh-python38 rh-python38-python-pip \
      ca-certificates \
    && update-ca-trust && yum clean all

# 5) Guarded /etc/bashrc: only source if files exist (prevents “No such file or directory”)
RUN { \
      echo '[ -r /etc/profile.d/modules.sh ] && source /etc/profile.d/modules.sh'; \
      echo '[ -r /opt/rh/devtoolset-3/enable ] && source /opt/rh/devtoolset-3/enable'; \
      echo '[ -r /opt/rh/rh-python38/enable ] && source /opt/rh/rh-python38/enable'; \
    } >> /etc/bashrc

# Also expose SCL bins for non-login shells
ENV PATH=/opt/rh/devtoolset-3/root/usr/bin:/opt/rh/rh-python38/root/usr/bin:$PATH
ENV PY38=/opt/rh/rh-python38/root/usr/bin/python3.8
ENV PIP_NO_CACHE_DIR=1

# 6) Python deps (upgrade pip toolchain; Py3.8-compatible NumPy 1.24.4; CPU Torch 2.2.1)
RUN "$PY38" -m pip install --upgrade "pip>=23.2" "setuptools>=65" wheel && \
    "$PY38" -m pip install numpy==1.24.4 && \
    "$PY38" -m pip install --prefer-binary \
      --extra-index-url https://download.pytorch.org/whl/cpu \
      torch==2.2.1 torchvision==0.17.1

# 7) Bowtie2 2.2.6
WORKDIR /opt
RUN wget -q https://sourceforge.net/projects/bowtie-bio/files/bowtie2/2.2.6/bowtie2-2.2.6-linux-x86_64.zip \
 && unzip -q bowtie2-2.2.6-linux-x86_64.zip \
 && rm -f bowtie2-2.2.6-linux-x86_64.zip

# 8) HTSlib 1.9 + Samtools 1.9
WORKDIR /tmp/build
RUN wget -q https://github.com/samtools/htslib/releases/download/1.9/htslib-1.9.tar.bz2 \
 && tar -xf htslib-1.9.tar.bz2 && cd htslib-1.9 \
 && ./configure --prefix=/opt/htslib-1.9 && make -j"$(nproc)" && make install \
 && cd .. && rm -rf htslib-1.9 htslib-1.9.tar.bz2 \
 && wget -q https://github.com/samtools/samtools/releases/download/1.9/samtools-1.9.tar.bz2 \
 && tar -xf samtools-1.9.tar.bz2 && cd samtools-1.9 \
 && ./configure --with-htslib=/opt/htslib-1.9 --prefix=/opt/samtools-1.9 \
 && make -j"$(nproc)" && make install \
 && cd .. && rm -rf samtools-1.9 samtools-1.9.tar.bz2

# 9) Modulefiles (write without login shell so /etc/bashrc doesn’t run)
RUN mkdir -p /etc/modulefiles/gnu /etc/modulefiles/bowtie /etc/modulefiles/samtools && \
    cat > /etc/modulefiles/gnu/4.9.2 <<'EOF' && \
    cat > /etc/modulefiles/bowtie/2.2.6 <<'EOF2' && \
    cat > /etc/modulefiles/samtools/1.9 <<'EOF3'
#%Module1.0
module-whatis "GCC 4.9.x from devtoolset-3"
set root /opt/rh/devtoolset-3/root
prepend-path PATH            $root/usr/bin
prepend-path LD_LIBRARY_PATH $root/usr/lib64
prepend-path MANPATH         $root/usr/share/man
prepend-path PKG_CONFIG_PATH $root/usr/lib64/pkgconfig
setenv CC gcc
setenv CXX g++
EOF
#%Module1.0
module-whatis "Bowtie2 2.2.6"
set root /opt/bowtie2-2.2.6
prepend-path PATH $root
EOF2
#%Module1.0
module-whatis "Samtools 1.9 (HTSlib 1.9)"
set root /opt/samtools-1.9
prepend-path PATH            $root/bin
prepend-path LD_LIBRARY_PATH /opt/htslib-1.9/lib
EOF3

# 10) Your custom slurm.conf (AccountingStorageType=accounting_storage/none)
RUN mkdir -p /etc/slurm /var/log/slurm /var/spool/slurmctld /var/spool/slurmd && \
    chown -R slurm:slurm /var/log/slurm /var/spool/slurmctld /var/spool/slurmd
COPY slurm.conf /etc/slurm/slurm.conf
RUN chown slurm:slurm /etc/slurm/slurm.conf && chmod 0644 /etc/slurm/slurm.conf

# 11) OpenSSH server (keep 22 inside; map host 2220 → 22 at runtime)
RUN ssh-keygen -A \
 && sed -ri 's/^#?PermitRootLogin.*/PermitRootLogin yes/' /etc/ssh/sshd_config \
 && sed -ri 's/^#?PasswordAuthentication.*/PasswordAuthentication no/' /etc/ssh/sshd_config \
 && echo 'root:root' | chpasswd

# Supervisord program so sshd runs alongside Slurm daemons
RUN mkdir -p /etc/supervisord.d && \
    printf '[program:sshd]\ncommand=/usr/sbin/sshd -D\nautostart=true\nautorestart=true\npriority=5\n' > /etc/supervisord.d/sshd.ini

# Convenience PATH
ENV PATH=/opt/bowtie2-2.2.6:/opt/samtools-1.9/bin:$PATH

WORKDIR /
