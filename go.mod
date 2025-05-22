module archive-manager

go 1.24

require (
	github.com/rs/zerolog v1.34.0
	github.com/xssnick/tonutils-go v1.12.0
)

require (
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.19 // indirect
	github.com/oasisprotocol/curve25519-voi v0.0.0-20220328075252-7dd334e3daae // indirect
	github.com/sigurn/crc16 v0.0.0-20211026045750-20ab5afb07e3 // indirect
	golang.org/x/crypto v0.38.0 // indirect
	golang.org/x/sys v0.33.0 // indirect
)

replace github.com/xssnick/tonutils-go v1.12.0 => ../tonutils-go
