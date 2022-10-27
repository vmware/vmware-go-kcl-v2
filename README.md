# VMWare Go KCL v2

![technology Go](https://img.shields.io/badge/technology-go-blue.svg)
[![Go Report Card](https://goreportcard.com/badge/github.com/vmware/vmware-go-kcl-v2)](https://goreportcard.com/report/github.com/vmware/vmware-go-kcl-v2)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![vmware-go-kcl-v2](https://github.com/vmware/vmware-go-kcl-v2/actions/workflows/vmware-go-kcl-v2-ci.yml/badge.svg)](https://github.com/vmware/vmware-go-kcl-v2/actions/workflows/vmware-go-kcl-v2-ci.yml)

## Overview

VMware-Go-KCL-V2 is a native open-source Go library for Amazon Kinesis Data Stream (KDS) consumption. It allows developers
to program KDS consumers in lightweight Go language and still take advantage of the features presented by the native
KDS Java API libraries.

[vmware-go-kcl-v2](https://github.com/vmware/vmware-go-kcl-v2) is a VMWare originated open-source project for AWS Kinesis
Client Library in Go. Within VMware, we have seen adoption in vSecureState and Carbon Black. In addition, Carbon Black
has contributed to the vmware-go-kcl codebase and heavily used it in the product. Besides,
[vmware-go-kcl-v2](https://github.com/vmware/vmware-go-kcl-v2) has got
[recognition](https://www.linkedin.com/posts/adityakrish_vmware-go-kcl-a-native-open-source-go-programming-activity-6810626798133616640-B6W8/),
and [contributions](https://github.com/vmware/vmware-go-kcl-v2/graphs/contributors) from the industry.

`vmware-go-kcl-v2` is the v2 version of VMWare KCL for the Go programming language by utilizing [AWS Go SDK V2](https://github.com/aws/aws-sdk-go-v2).

## Try it out

### Prerequisites

* [aws-sdk-go-v2](https://github.com/aws/aws-sdk-go-v2)
* The v2 SDK requires a minimum version of `Go 1.17`.
* [gosec](https://github.com/securego/gosec)

### Build & Run

1. Initialize Project

2. Build
    > `make build`

3. Test
    > `make test`

## Documentation

VMware-Go-KCL matches exactly the same interface and programming model from original Amazon KCL, the best place for getting reference, tutorial is from Amazon itself:

* [Developing Consumers Using the Kinesis Client Library](https://docs.aws.amazon.com/streams/latest/dev/developing-consumers-with-kcl.html)
* [Troubleshooting](https://docs.aws.amazon.com/streams/latest/dev/troubleshooting-consumers.html)
* [Advanced Topics](https://docs.aws.amazon.com/streams/latest/dev/advanced-consumers.html)

## Contributing

The vmware-go-kcl-v2 project team welcomes contributions from the community. Before you start working with vmware-go-kcl-v2, please
read our [Developer Certificate of Origin](https://cla.vmware.com/dco). All contributions to this repository must be
signed as described on that page. Your signature certifies that you wrote the patch or have the right to pass it on
as an open-source patch. For more detailed information, refer to [CONTRIBUTING.md](CONTRIBUTING.md).

## License

MIT License
