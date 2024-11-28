using Cxx = import "./include/c++.capnp";
$Cxx.namespace("cereal");

@0xb526ba661d550a59;

# custom.capnp: a home for empty structs reserved for custom forks
# These structs are guaranteed to remain reserved and empty in mainline
# cereal, so use these if you want custom events in your fork.

# you can rename the struct, but don't change the identifier
struct Ioniq @0x81c2f05a394cf4af {
    altitudeMsl @0 :Float64;

    chargingType @1 :ChargingType;
    enum ChargingType {
        notCharging @0;
        ac @1;
        dc @2;
        other @3;
    }

    voltage @2 :Float32;
    current @3 :Float32;
    maxTemp @4 :Int8;
    minTemp @5 :Int8;
    inletTemp @6 :Int8;
    heaterTemp @7 :Int8;

    remainingEnergy @8 :Float32;
    socDisplay @9 :Float32;
    availableChargePower @10 :Float32;
    availableDischargePower @11 :Float32;
}

struct CustomReserved1 @0xaedffd8f31e7b55d {
}

struct CustomReserved2 @0xf35cc4560bbf6ec2 {
}

struct CustomReserved3 @0xda96579883444c35 {
}

struct CustomReserved4 @0x80ae746ee2596b11 {
}

struct CustomReserved5 @0xa5cd762cd951a455 {
}

struct CustomReserved6 @0xf98d843bfd7004a3 {
}

struct CustomReserved7 @0xb86e6369214c01c8 {
}

struct CustomReserved8 @0xf416ec09499d9d19 {
}

struct CustomReserved9 @0xa1680744031fdb2d {
}
