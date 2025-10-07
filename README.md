<h1 align="center">
    <a href="https://authzed.com#gh-dark-mode-only" target="_blank">
        <img width="300" src="https://github.com/user-attachments/assets/0ebf4718-283b-4c40-b567-1d577c0a2e03" alt="spicedb logo">
    </a>
    <a href="https://authzed.com#gh-light-mode-only" target="_blank">
        <img width="300" src="https://github.com/user-attachments/assets/577a72f9-4fdd-49f8-b1d6-e53025d219b8" alt="spicedb Logo">
    </a>
</h1>

<h3 align="center">
  SpiceDB sets the standard for authorization that <i>scales</i>.
  <br/><br/>Scale with<br/>
  Traffic • Dev Velocity • Functionality • Geography
</h3>

<p align="center">
  <a href="https://github.com/authzed/spicedb/releases"><img alt="release badge" src="https://img.shields.io/github/v/release/authzed/spicedb?color=%236EC93F&label=latest%20release&sort=semver&style=flat-square"></a>
  &nbsp;
  <a href="https://hub.docker.com/repository/docker/authzed/spicedb" target="_blank"><img alt="docker pulls badge" src="https://img.shields.io/docker/pulls/authzed/spicedb?color=%23448CE6&style=flat-square"></a>
  &nbsp;
  <a href="https://authzed.com/blog/go-ecosystem"><img alt="built with Go badge" src="https://img.shields.io/badge/built_with-Go-367B99.svg?style=flat-square"></a>
  &nbsp;
  <a href="https://app.codecov.io/gh/authzed/spicedb"><img alt="coverage" src="https://img.shields.io/codecov/c/github/authzed/spicedb"></a>
  &nbsp;
  <a href="https://www.bestpractices.dev/en/projects/6348" target="_blank"><img alt="cii badge" src="https://img.shields.io/cii/percentage/6348?style=flat-square&label=cii%20best%20practices&color=F8D44B"></a>
  &nbsp;
  <a href="https://securityscorecards.dev/viewer/?uri=github.com/authzed/spicedb" target="_blank"><img alt="ssf badge" src="https://api.securityscorecards.dev/projects/github.com/authzed/spicedb/badge"></a>
</p>

<p align="center">
  <a href="https://authzed.com/discord"><img alt="discord badge" src="https://img.shields.io/badge/discord-spicedb-7289da?style=flat-square"></a>
	&nbsp;
    <a href="https://twitter.com/authzed"><img alt="twitter badge" src="https://img.shields.io/badge/twitter-@authzed-1d9bf0.svg?style=flat-square"></a>
    &nbsp;
    <a href="https://www.linkedin.com/company/authzed/"><img alt="linkedin badge" src="https://img.shields.io/badge/linkedin-+authzed-2D65BC.svg?style=flat-square"></a>
</p>

<p align="center">
  <a href="https://github.com/codespaces/new?hide_repo_select=true&ref=main&repo=396856161" target="_blank"><img alt="launch codespaces badge" src="https://img.shields.io/badge/launch-Codespaces-blue?style=flat-square"></a>
  &nbsp;
  <a href="https://gitpod.io/#https://github.com/authzed/spicedb" target="_blank"><img alt="launch gitpod badge" src="https://img.shields.io/badge/Gitpod-ready--to--code-blue?style=flat-square"></a>
</p>

## What is SpiceDB?

<a href="https://authzed.com#gh-dark-mode-only" target="_blank"><img align="right" width="300" src="https://github.com/user-attachments/assets/e0e70549-91dc-4a07-9309-2e18942a7902" alt="spicedb diagram" /></a>
<a href="https://authzed.com#gh-light-mode-only" target="_blank"><img align="right" width="300" src="https://github.com/user-attachments/assets/97342fce-d94b-42b6-b02f-fe3958d13981" alt="spicedb diagram" /></a>

SpiceDB is the most mature open source project inspired by Google's internal authorization system: [Zanzibar].

As of 2021, [broken access control became the #1 threat to web security according to OWASP][owasp].
With SpiceDB, platform and product teams can be be protected by answering this question easily: **"can subject X perform action Y on resource Z?"**

Similar to a relational database, developers define a **schema**, write data in the form of **relationships**, and then use SpiceDB's clients to issue **permission checks** in their application to determine what actions a user can take on a resource. Other queries are also possible, such as "What can `subject` do?" or "Who can access `resource`?".

SpiceDB is often ran as a centralized service shared across product suites and microservice architectures.

SpiceDB is focused purely on *authorization* and is designed to be fully agnostic to *authentication* solutions/identity providers.

[owasp]: https://owasp.org/Top10/A01_2021-Broken_Access_Control/

### What is Google Zanzibar?

In 2019, Google released the paper "[Zanzibar: Google's Consistent, Global Authorization System][zanzibar]" providing the original inspiration for SpiceDB.
The paper presents the design, implementation, and deployment of, Zanzibar, Google's internal system for storing and evaluating access control lists.
Originally designed for [Google+ Circles][circles], Zanzibar now sits at the core Google's entire product suite (Calendar, Drive, Maps, Photos, YouTube) and powers the Google Cloud IAM service.

While SpiceDB has gone on to innovate well beyond the functionality outlined in the paper, development of SpiceDB aims to always remain faithful to the paper's values and goals.

[zanzibar]: https://authzed.com/zanzibar
[circles]: https://en.wikipedia.org/wiki/Google+#Circles

### Why SpiceDB?

- [**World-class engineering**][about]: painstakingly built by experts that pioneered the cloud-native ecosystem
- [**Authentic design**][zanzibar]: mature and feature-complete implementation of Google's Zanzibar paper
- [**Proven in production**][1M]: 5ms p95 when scaled to millions of queries/s, billions of relationships
- [**Global consistency**][consistency]: consistency configured per-request unlocks correctness while maintaining performance
- [**Multi-paradigm**][caveats]: caveated relationships combine the best concepts in authorization: ABAC & ReBAC
- [**Safety in tooling**][tooling]: designs schemas with real-time validation or validate in your CI/CD workflow
- [**Reverse Indexes**][reverse-indexes]: queries for "What can `subject` do?", "Who can access `resource`?"

[about]: https://authzed.com/why-authzed
[1M]: https://authzed.com/blog/google-scale-authorization
[caveats]: https://netflixtechblog.com/abac-on-spicedb-enabling-netflixs-complex-identity-types-c118f374fa89
[tooling]: https://authzed.com/docs/spicedb/modeling/validation-testing-debugging
[reverse-indexes]: https://authzed.com/docs/spicedb/getting-started/faq#what-is-a-reverse-index
[consistency]: https://authzed.com/docs/spicedb/concepts/consistency

### Who uses SpiceDB?

SpiceDB is a powerful tool in a variety of domains and in organizations of all sizes; we've chosen to highlight a few interesting community members:

- [IBM's AI Data & Model Factory Platform](https://youtu.be/4K2a9HcRhXA)
- [Red Hat's Insights Platform](https://www.redhat.com/en/technologies/management/insights)
- [GitPod](https://github.com/gitpod-io/gitpod/issues/15632)
- [TubiTV China (中文)](https://zhuanlan.zhihu.com/p/685603356)
- [DMM Online Salon (日本語)](https://inside.dmm.com/articles/salon-datebase-migration-challenges/)

Beyond the community, you can also read [customer stories][stories] for commercial usage of SpiceDB.

[stories]: https://authzed.com/customers

## Getting Started

### Installing the binary

Binary releases are available for Linux, macOS, and Windows on AMD64 and ARM64 architectures.

[Homebrew] users for both macOS and Linux can install the latest binary releases of SpiceDB and [zed] using the official tap:

```command
brew install authzed/tap/spicedb authzed/tap/zed
```

[Debian-based Linux] users can install SpiceDB packages by adding a new APT source:

```command
sudo apt update && sudo apt install -y curl ca-certificates gpg
curl https://pkg.authzed.com/apt/gpg.key | sudo apt-key add -
sudo echo "deb https://pkg.authzed.com/apt/ * *" > /etc/apt/sources.list.d/fury.list
sudo apt update && sudo apt install -y spicedb zed
```

[RPM-based Linux] users can install SpiceDB packages by adding a new YUM repository:

```command
sudo cat << EOF >> /etc/yum.repos.d/Authzed-Fury.repo
[authzed-fury]
name=AuthZed Fury Repository
baseurl=https://pkg.authzed.com/yum/
enabled=1
gpgcheck=0
EOF
sudo dnf install -y spicedb zed
```

[zed]: https://github.com/authzed/zed
[homebrew]: https://docs.authzed.com/spicedb/installing#brew
[Debian-based Linux]: https://en.wikipedia.org/wiki/List_of_Linux_distributions#Debian-based
[RPM-based Linux]: https://en.wikipedia.org/wiki/List_of_Linux_distributions#RPM-based
  
### Running a container

Container images are available for AMD64 and ARM64 architectures on the following registries:

- [authzed/spicedb](https://hub.docker.com/r/authzed/spicedb)
- [ghcr.io/authzed/spicedb](https://github.com/authzed/spicedb/pkgs/container/spicedb)
- [quay.io/authzed/spicedb](https://quay.io/authzed/spicedb)

[Docker] users can run the latest SpiceDB container with the following:

```shell
# expose grpc and http. http is used in the examples below.
docker run --rm -p 50051:50051 -p 8443:8443 authzed/spicedb serve --http-enabled true --grpc-preshared-key "somerandomkeyhere"
```

SpiceDB containers use [Chainguard Images] to ship the bare minimum userspace which is a huge boon to security, but can complicate debugging.
If you want to execute a user session into a running SpiceDB container and install packages, you can use one of our debug images.

Appending `-debug` to any tag will provide you an image that has a userspace with debug tooling:

```command
docker run --rm -ti --entrypoint sh authzed/spicedb:latest-debug
```

Containers are also available for each git commit to the `main` branch under `${REGISTRY}/authzed/spicedb-git:${COMMIT}`.

[Docker]: https://docs.docker.com/get-docker/
[Chainguard Images]: https://github.com/chainguard-images/images

### Write your own schema and relationships

Now that you have SpiceDB running, you must define your **schema** and write **relationships** that represent the permissions in your application. There are various way to do this:

- Use the [client libraries].
- Use the [hosted Playground] or [run it yourself locally]. The playground contains a set of example schemas and test data for different scenarios.
- Use the [zed] CLI.
- Use the [gRPC](https://buf.build/authzed/api/docs/main:authzed.api.v1) or the [HTTP](https://authzed.com/docs/spicedb/api/http-api) APIs. For example:
  
```shell
    # write a schema
    curl --location 'http://localhost:8443/v1/schema/write' \
    --header 'Content-Type: application/json' \
    --header 'Accept: application/json' \
    --header 'Authorization: Bearer somerandomkeyhere' \
    --data '{
        "schema": "definition user {} \n definition folder { \n relation parent: folder\n relation viewer: user \n permission view = viewer + parent->view \n } \n definition document {\n relation folder: folder \n relation viewer: user \n permission view = viewer + folder->view \n }"
    }'

    # write a relationship
    curl --location 'http://localhost:8443/v1/relationships/write' \
    --header 'Content-Type: application/json' \
    --header 'Accept: application/json' \
    --header 'Authorization: Bearer somerandomkeyhere' \
    --data '{
        "updates": [
            {
                "operation": "OPERATION_TOUCH",
                "relationship": {
                    "resource": {
                        "objectType": "folder",
                        "objectId": "budget"
                    },
                    "relation": "viewer",
                    "subject": {
                        "object": {
                            "objectType": "user",
                            "objectId": "anne"
                        }
                    }
                }
            }
        ]
    }'
```

You can follow a [guide for developing a schema] or review the the schema language [design documentation].

Finally, you can watch the [SpiceDB primer video on schema development](https://www.youtube.com/watch?v=AoK0LrkGFDY).

[hosted Playground]: https://play.authzed.com
[run it yourself locally]: https://github.com/authzed/playground
[Playground]: https://github.com/authzed/playground
[guide for developing a schema]: https://docs.authzed.com/guides/schema
[design documentation]: https://docs.authzed.com/reference/schema-lang

### Query the SpiceDB API

You can use the [client libraries] or the [gRPC](https://buf.build/authzed/api/docs/main:authzed.api.v1) and [HTTP](https://authzed.com/docs/spicedb/api/http-api) APIs to query SpiceDB. For example,

```shell
curl --location 'http://localhost:8443/v1/permissions/check' \
--header 'Content-Type: application/json' \
--header 'Accept: application/json' \
--header 'Authorization: Bearer somerandomkeyhere' \
--data '{
  "consistency": {
    "minimizeLatency": true
  },
  "resource": {
    "objectType": "folder",
    "objectId": "budget"
  },
  "permission": "view",
  "subject": {
    "object": {
      "objectType": "user",
      "objectId": "anne"
    }
  }
}'


#{
#    "checkedAt": {
#        "token": "GhUKEzE3NTE1NjYwMjUwMDAwMDAwMDA="
#    },
#    "permissionship": "PERMISSIONSHIP_HAS_PERMISSION"
#}

'
```

You can also issue queries with [zed], the official command-line client.
The [Playground] also has a tab for experimenting with `zed` all from within your browser.

### Integrating SpiceDB into Your Application

To get an understanding of integrating an application with SpiceDB, you can follow the [Protecting Your First App] guide or review API documentation on the [Buf Registry] or [Postman].

[client libraries]: https://github.com/authzed/awesome-spicedb#clients
[Protecting Your First App]: https://docs.authzed.com/guides/first-app
[Buf Registry]: https://buf.build/authzed/api/docs
[Postman]: https://www.postman.com/authzed/workspace/spicedb/overview

### Deploying to Production

The core SpiceDB service has been utilized in production by Authzed since 2021 so you can be confident that it is battle-tested. Moreover, it supports various datastores, including Google Cloud Spanner, CockroachDB, MySQL, and PostgreSQL. Read [this](https://authzed.com/docs/spicedb/concepts/operator) to learn the best practices for each.

You can choose to self-host SpiceDB, or use [AuthZed Cloud], a fully managed service. See a [comparison of the various options](https://authzed.com/docs/authzed/guides/picking-a-product).

If you choose to self-host, we recommend deploying SpiceDB using Kubernetes. If you're only experimenting, feel free to try out one of our community-maintained [examples] for [testing SpiceDB on Kubernetes]:

```command
kubectl apply -f https://raw.githubusercontent.com/authzed/examples/main/kubernetes/example.yaml
```

For a more detailed guide on the SpiceDB Kubernetes Operator, see [this](https://authzed.com/docs/spicedb/ops/operator).

For more best practices on deploying SpiceDB, read our [best practices guide].

[examples]: https://github.com/authzed/examples
[testing SpiceDB on Kubernetes]: https://github.com/authzed/examples/tree/main/kubernetes
[AuthZed Cloud]: https://authzed.com/cloud
[best practices guide]: https://authzed.com/docs/best-practices

#### Telemetry

SpiceDB collects anonymous telemetry data to help us understand how the community is using SpiceDB and to help us prioritize features. This telemetry is opt-out and can be disabled via setting `--telemetry-endpoint=""`. For more information on the telemetry we collect, see [telemetry](./TELEMETRY.md).

## More Resources

- Ask questions via [GitHub Discussions] or our [Community Discord]
- Read [blog posts] from the Authzed team describing the project and major announcements
- Follow our [self-guided workshops] on usecases ranging from Updating Web Views, to Securing RAG Pipelines
- Watch our [YouTube videos] about SpiceDB, modeling schemas, leveraging CNCF projects, and more
- Explore the [SpiceDB Awesome List] that enumerates official and third-party projects built by the community
- Reference [community examples] for demo environments, integration testing, CI pipelines, and writing schemas

[GitHub Discussions]: https://github.com/orgs/authzed/discussions/new?category=q-a
[Community Discord]: https://authzed.com/discord
[blog posts]: https://authzed.com/blog
[self-guided workshops]: https://github.com/authzed/workshops/
[SpiceDB Awesome List]: https://github.com/authzed/awesome-spicedb
[YouTube videos]: https://www.youtube.com/@authzed
[community examples]: https://github.com/authzed/examples

## Join the Community

Join our fellow contributors from companies such as <img alt="github logo" height="15px" src="https://github.com/authzed/spicedb/assets/343539/c05b8aef-c862-4499-bebf-0a43f3b423c4"> GitHub, <img alt="adobe logo" height="15px" src="https://github.com/user-attachments/assets/64007fa0-f342-4eba-bc81-0e078677e918"> Adobe, <img alt="google logo" height="15px" src="https://github.com/user-attachments/assets/15a144f4-4244-40af-be58-53ea3be46bc4"> Google, <img alt="fastly logo" height="15px" src="https://github.com/user-attachments/assets/57f042c3-e106-4524-95b8-167acc6be16e"> Fastly, <img alt="plaid logo" height="15px" src="https://github.com/user-attachments/assets/0678972d-b8ff-41e6-a507-7289a1ee1e94"> Plaid, <img alt="red hat logo" height="15px" src="https://github.com/user-attachments/assets/955a6c28-3a5c-4679-8e12-b50734024be2"> Red Hat, and <img alt="reddit logo" height="15px" src="https://github.com/user-attachments/assets/78d542f9-37f7-4d78-bf34-8aa0b0ddd12d"> Reddit.

SpiceDB is a community project where everyone is invited to participate and [feel welcomed].
While the project has a technical goal, participation is not restricted to those with code contributions.

[CONTRIBUTING.md] documents communication, contribution flow, legal requirements, and common tasks when contributing to the project.

You can find issues by priority: [Urgent], [High], [Medium], [Low], [Maybe].
There are also [good first issues].

Our [documentation] is also [open source][oss-docs] if you'd like to clarify anything you find confusing.

[feel welcomed]: CODE-OF-CONDUCT.md
[CONTRIBUTING.md]: CONTRIBUTING.md
[Urgent]: https://github.com/authzed/spicedb/labels/priority%2F0%20urgent
[High]: https://github.com/authzed/spicedb/labels/priority%2F1%20high
[Medium]: https://github.com/authzed/spicedb/labels/priority%2F2%20medium
[Low]: https://github.com/authzed/spicedb/labels/priority%2F3%20low
[Maybe]: https://github.com/authzed/spicedb/labels/priority%2F4%20maybe
[good first issues]: https://github.com/authzed/spicedb/labels/hint%2Fgood%20first%20issue
[documentation]: https://authzed.com/docs
[oss-docs]: https://github.com/authzed

## Acknowledgements

SpiceDB is a community project fueled by contributions from both organizations and individuals.
We appreciate all contributions, large and small, and would like to thank all those involved.

In addition, we'd like to highlight a few notable contributions:

- <img alt="github logo" height="15px" src="https://github.com/authzed/spicedb/assets/343539/c05b8aef-c862-4499-bebf-0a43f3b423c4"> The GitHub Authorization Team for implementing and contributing the MySQL datastore
- <img alt="netflix logo" height="15px" src="https://github.com/authzed/spicedb/assets/343539/e64128f0-978f-4fd6-bdd7-1ce7cb6b34b9"> The Netflix Authorization Team for sponsoring and being a design partner for caveats
- <img alt="equinix logo" height="15px" src="https://github.com/authzed/spicedb/assets/343539/7bf706f9-910d-4902-8957-c914a7468eff"> The Equinix Metal Team for sponsoring our benchmarking hardware
