#!/bin/bash
archs=("arm64" "amd64")
for arch in ${archs[@]}; do
    go env -w GOARCH=${arch} GOOS=linux
    go build -mod=vendor  -o stress_${arch} ./main.go
done	
