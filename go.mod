module github.com/gardener/etcd-wrapper

go 1.20

// These are test-only dependencies
require github.com/onsi/gomega v1.27.5

require (
	// Etcd issue Ref: https://github.com/etcd-io/etcd/issues/12068
	// Etcd 3.4.x vendoring issue Ref: https://github.com/etcd-io/etcd/issues/11154#issuecomment-677940701
	go.etcd.io/etcd v0.0.0-20240320135013-950cd5fbe6ca
	go.uber.org/zap v1.24.0
)

require (
	github.com/benbjohnson/clock v1.1.0 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/coreos/go-semver v0.2.0 // indirect
	github.com/coreos/go-systemd v0.0.0-20180511133405-39ca1b05acc7 // indirect
	github.com/coreos/pkg v0.0.0-20160727233714-3ac0863d7acf // indirect
	github.com/dustin/go-humanize v0.0.0-20171111073723-bb3d318650d4 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang-jwt/jwt v3.2.1+incompatible // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/google/btree v1.0.0 // indirect
	github.com/google/go-cmp v0.5.9 // indirect
	github.com/gorilla/websocket v1.4.2 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.0.1-0.20190118093823-f849b5445de4 // indirect
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway v1.11.0 // indirect
	github.com/jonboulle/clockwork v0.1.0 // indirect
	github.com/json-iterator/go v1.1.11 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.1 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.1 // indirect
	github.com/prometheus/client_golang v1.11.1 // indirect
	github.com/prometheus/client_model v0.2.0 // indirect
	github.com/prometheus/common v0.26.0 // indirect
	github.com/prometheus/procfs v0.6.0 // indirect
	github.com/sirupsen/logrus v1.9.3 // indirect
	github.com/soheilhy/cmux v0.1.5 // indirect
	github.com/spf13/pflag v1.0.1 // indirect
	github.com/tmc/grpc-websocket-proxy v0.0.0-20200427203606-3cfed13b9966 // indirect
	github.com/xiang90/probing v0.0.0-20190116061207-43a291ad63a2 // indirect
	go.etcd.io/bbolt v1.3.9 // indirect
	go.uber.org/atomic v1.7.0 // indirect
	go.uber.org/multierr v1.6.0 // indirect
	golang.org/x/crypto v0.17.0 // indirect
	golang.org/x/net v0.17.0 // indirect
	golang.org/x/sys v0.15.0 // indirect
	golang.org/x/text v0.14.0 // indirect
	golang.org/x/time v0.0.0-20180412165947-fbb02b2291d2 // indirect
	google.golang.org/genproto v0.0.0-20230711160842-782d3b101e98 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20230711160842-782d3b101e98 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20230711160842-782d3b101e98 // indirect
	google.golang.org/grpc v1.58.3 // indirect
	google.golang.org/protobuf v1.33.0 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	sigs.k8s.io/yaml v1.1.0 // indirect
)
