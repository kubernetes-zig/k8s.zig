K8S_VERSION ?= v0.35.0
K8S_APIMACHINERY_REPO = https://raw.githubusercontent.com/kubernetes/apimachinery/$(K8S_VERSION)
K8S_API_REPO = https://raw.githubusercontent.com/kubernetes/api/$(K8S_VERSION)

PROTOC ?= protoc
ZIG_PROTOBUF_DIR ?= ../zig-protobuf
PROTOC_GEN_ZIG ?= $(ZIG_PROTOBUF_DIR)/zig-out/bin/protoc-gen-zig

PROTO_DIR = proto
API_DIR = api

# Apimachinery proto files (dependency order)
APIMACHINERY_PROTOS = \
	k8s.io/apimachinery/pkg/runtime/generated.proto \
	k8s.io/apimachinery/pkg/runtime/schema/generated.proto \
	k8s.io/apimachinery/pkg/util/intstr/generated.proto \
	k8s.io/apimachinery/pkg/api/resource/generated.proto \
	k8s.io/apimachinery/pkg/apis/meta/v1/generated.proto

# Add more API groups here as needed:
# 	k8s.io/api/core/v1/generated.proto \
# 	k8s.io/api/apps/v1/generated.proto
API_PROTOS = \
	k8s.io/api/apidiscovery/v2/generated.proto

ALL_PROTOS = $(APIMACHINERY_PROTOS) $(API_PROTOS)

.PHONY: generate download-protos gen-zig fix-generated test clean-generated

## Build protoc-gen-zig from our fork
build-protoc-gen-zig:
	@echo "Building protoc-gen-zig..."
	@cd $(ZIG_PROTOBUF_DIR) && zig build 2>/dev/null
	@echo "Done."

## Run all code generation
generate: build-protoc-gen-zig download-protos gen-zig fix-generated

## Download K8s proto files
download-protos:
	@echo "Downloading K8s proto files ($(K8S_VERSION))..."
	@mkdir -p $(PROTO_DIR)/k8s.io/apimachinery/pkg/apis/meta/v1
	@mkdir -p $(PROTO_DIR)/k8s.io/apimachinery/pkg/runtime/schema
	@mkdir -p $(PROTO_DIR)/k8s.io/apimachinery/pkg/api/resource
	@mkdir -p $(PROTO_DIR)/k8s.io/apimachinery/pkg/util/intstr
	@curl -sL $(K8S_APIMACHINERY_REPO)/pkg/apis/meta/v1/generated.proto \
		-o $(PROTO_DIR)/k8s.io/apimachinery/pkg/apis/meta/v1/generated.proto
	@curl -sL $(K8S_APIMACHINERY_REPO)/pkg/runtime/generated.proto \
		-o $(PROTO_DIR)/k8s.io/apimachinery/pkg/runtime/generated.proto
	@curl -sL $(K8S_APIMACHINERY_REPO)/pkg/runtime/schema/generated.proto \
		-o $(PROTO_DIR)/k8s.io/apimachinery/pkg/runtime/schema/generated.proto
	@curl -sL $(K8S_APIMACHINERY_REPO)/pkg/api/resource/generated.proto \
		-o $(PROTO_DIR)/k8s.io/apimachinery/pkg/api/resource/generated.proto
	@curl -sL $(K8S_APIMACHINERY_REPO)/pkg/util/intstr/generated.proto \
		-o $(PROTO_DIR)/k8s.io/apimachinery/pkg/util/intstr/generated.proto
	@mkdir -p $(PROTO_DIR)/k8s.io/api/apidiscovery/v2
	@curl -sL $(K8S_API_REPO)/apidiscovery/v2/generated.proto \
		-o $(PROTO_DIR)/k8s.io/api/apidiscovery/v2/generated.proto
	@echo "Done."

## Generate Zig code from proto files
gen-zig:
	@echo "Generating Zig types from proto files..."
	@$(PROTOC) \
		--plugin=protoc-gen-zig=$(PROTOC_GEN_ZIG) \
		--zig_out=$(API_DIR) \
		--zig_opt=json_compat \
		--proto_path=$(PROTO_DIR) \
		$(addprefix $(PROTO_DIR)/,$(ALL_PROTOS))
	@echo "Done."

## Fix generated code for 0.16-dev compatibility
fix-generated:
	@echo "Fixing generated code for Zig 0.16-dev..."
	@find $(API_DIR) -name "*.pb.zig" -exec sed -i '' 's/	/    /g' {} +
	@echo "Done."

## Run tests
test:
	zig build test

## Remove generated files and downloaded protos
clean-generated:
	rm -rf $(API_DIR)/k8s $(PROTO_DIR)
