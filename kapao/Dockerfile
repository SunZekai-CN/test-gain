FROM nvcr.io/nvidia/l4t-pytorch:r35.1.0-pth1.12-py3

#初始化镜像
RUN apt-get update
RUN apt-get install -y vim git
RUN DEBIAN_FRONTEND=oninteractive apt-get install -y wget unzip python3-virtualenv
RUN python3 -m pip install --upgrade pip

RUN apt-get install -y build-essential libxmu-dev libxi-dev libgl-dev libosmesa-dev liblog4cplus-dev curl
WORKDIR /home
RUN git clone https://github.com/SunZekai-CN/GPU_offload_stable.git
RUN cd GPU_offload_stable && mkdir build && cd build && export GVIRTUS_HOME=/opt/GVirtuS && cmake .. && make && make install

#设置初始文件夹
WORKDIR /work