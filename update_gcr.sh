docker image build . --no-cache -t gcr.io/sandbox-220614/scsw/jenkins/imaging-ecosystem:0.1.1 --platform linux/amd64
docker image ls | head -2 | grep -v REPOSITORY | awk '{print $3}' | xargs echo
docker image ls | head -2 | grep -v REPOSITORY | awk '{print $3}' | xargs -J % -t docker image tag % gcr.io/sandbox-220614/scsw/jenkins/imaging-ecosystem:0.1.1
docker image ls | head -2 | grep -v REPOSITORY | awk '{print $3}' | xargs -J % -t docker image tag % gcr.io/sandbox-220614/scsw/jenkins/imaging-ecosystem:latest
# gcloud auth login
# gcloud auth configure-docker
CLOUDSDK_CORE_PROJECT=sandbox-220614
gcloud config set project sandbox-220614
docker push gcr.io/sandbox-220614/scsw/jenkins/imaging-ecosystem:0.1.1
docker push gcr.io/sandbox-220614/scsw/jenkins/imaging-ecosystem:latest
