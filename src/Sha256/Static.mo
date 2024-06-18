import Array "mo:base/Array";
import Blob "mo:base/Blob";
import Nat8 "mo:base/Nat8";
import Nat32 "mo:base/Nat32";
import Nat64 "mo:base/Nat64";
import Prim "mo:prim";

module Sha256 {
  public type Algorithm = { #sha224; #sha256 };

  let K00 : Nat32 = 0x428a2f98;
  let K01 : Nat32 = 0x71374491;
  let K02 : Nat32 = 0xb5c0fbcf;
  let K03 : Nat32 = 0xe9b5dba5;
  let K04 : Nat32 = 0x3956c25b;
  let K05 : Nat32 = 0x59f111f1;
  let K06 : Nat32 = 0x923f82a4;
  let K07 : Nat32 = 0xab1c5ed5;
  let K08 : Nat32 = 0xd807aa98;
  let K09 : Nat32 = 0x12835b01;
  let K10 : Nat32 = 0x243185be;
  let K11 : Nat32 = 0x550c7dc3;
  let K12 : Nat32 = 0x72be5d74;
  let K13 : Nat32 = 0x80deb1fe;
  let K14 : Nat32 = 0x9bdc06a7;
  let K15 : Nat32 = 0xc19bf174;
  let K16 : Nat32 = 0xe49b69c1;
  let K17 : Nat32 = 0xefbe4786;
  let K18 : Nat32 = 0x0fc19dc6;
  let K19 : Nat32 = 0x240ca1cc;
  let K20 : Nat32 = 0x2de92c6f;
  let K21 : Nat32 = 0x4a7484aa;
  let K22 : Nat32 = 0x5cb0a9dc;
  let K23 : Nat32 = 0x76f988da;
  let K24 : Nat32 = 0x983e5152;
  let K25 : Nat32 = 0xa831c66d;
  let K26 : Nat32 = 0xb00327c8;
  let K27 : Nat32 = 0xbf597fc7;
  let K28 : Nat32 = 0xc6e00bf3;
  let K29 : Nat32 = 0xd5a79147;
  let K30 : Nat32 = 0x06ca6351;
  let K31 : Nat32 = 0x14292967;
  let K32 : Nat32 = 0x27b70a85;
  let K33 : Nat32 = 0x2e1b2138;
  let K34 : Nat32 = 0x4d2c6dfc;
  let K35 : Nat32 = 0x53380d13;
  let K36 : Nat32 = 0x650a7354;
  let K37 : Nat32 = 0x766a0abb;
  let K38 : Nat32 = 0x81c2c92e;
  let K39 : Nat32 = 0x92722c85;
  let K40 : Nat32 = 0xa2bfe8a1;
  let K41 : Nat32 = 0xa81a664b;
  let K42 : Nat32 = 0xc24b8b70;
  let K43 : Nat32 = 0xc76c51a3;
  let K44 : Nat32 = 0xd192e819;
  let K45 : Nat32 = 0xd6990624;
  let K46 : Nat32 = 0xf40e3585;
  let K47 : Nat32 = 0x106aa070;
  let K48 : Nat32 = 0x19a4c116;
  let K49 : Nat32 = 0x1e376c08;
  let K50 : Nat32 = 0x2748774c;
  let K51 : Nat32 = 0x34b0bcb5;
  let K52 : Nat32 = 0x391c0cb3;
  let K53 : Nat32 = 0x4ed8aa4a;
  let K54 : Nat32 = 0x5b9cca4f;
  let K55 : Nat32 = 0x682e6ff3;
  let K56 : Nat32 = 0x748f82ee;
  let K57 : Nat32 = 0x78a5636f;
  let K58 : Nat32 = 0x84c87814;
  let K59 : Nat32 = 0x8cc70208;
  let K60 : Nat32 = 0x90befffa;
  let K61 : Nat32 = 0xa4506ceb;
  let K62 : Nat32 = 0xbef9a3f7;
  let K63 : Nat32 = 0xc67178f2;

  public let rot = Nat32.bitrotRight;
  
  public let nat64To32 = Prim.nat64ToNat32;
  public let nat32To16 = Prim.nat32ToNat16;
  public let nat32To64 = Prim.nat32ToNat64;
  public let nat16To32 = Prim.nat16ToNat32;
  public let nat16To8 = Prim.nat16ToNat8;
  public let nat8To16 = Prim.nat8ToNat16;
  
  public type StaticSha256 = {
    algo : Algorithm;
    msg : [var Nat16];
    digest : [var Nat8];
    var i_msg : Nat8;
    var i_block : Nat32;
    var high : Bool;
    var word : Nat16;

    // state variables in Nat16 form
    sh : [var Nat16];
    sl : [var Nat16];
  };

  public func Digest(algo: Algorithm) : StaticSha256 {
    let state : StaticSha256 =  {
      algo;
      msg = Array.init<Nat16>(32, 0);
      digest = switch (algo) {
        case (#sha224) Array.init<Nat8>(28, 0);
        case (#sha256) Array.init<Nat8>(32, 0);
      };

      var i_msg = 0;
      var i_block = 0;
      var high = true;
      var word = 0;

      // state variables in Nat16 form
      sh = Array.init<Nat16>(8, 0);
      sl = Array.init<Nat16>(8, 0);
    };

    reset(state);

    return state;
  };

  public func algo(state: StaticSha256) : Algorithm = state.algo;

  public func reset(state: StaticSha256) {
    state.i_msg := 0;
    state.i_block := 0;
    state.high := true;

    if (state.algo == #sha224){
      state.sh[0] := 0xc105; state.sl[0] := 0x9ed8;
      state.sh[1] := 0x367c; state.sl[1] := 0xd507;
      state.sh[2] := 0x3070; state.sl[2] := 0xdd17;
      state.sh[3] := 0xf70e; state.sl[3] := 0x5939;
      state.sh[4] := 0xffc0; state.sl[4] := 0x0b31;
      state.sh[5] := 0x6858; state.sl[5] := 0x1511;
      state.sh[6] := 0x64f9; state.sl[6] := 0x8fa7;
      state.sh[7] := 0xbefa; state.sl[7] := 0x4fa4;
    } else {
      state.sh[0] := 0x6a09; state.sl[0] := 0xe667;
      state.sh[1] := 0xbb67; state.sl[1] := 0xae85;
      state.sh[2] := 0x3c6e; state.sl[2] := 0xf372;
      state.sh[3] := 0xa54f; state.sl[3] := 0xf53a;
      state.sh[4] := 0x510e; state.sl[4] := 0x527f;
      state.sh[5] := 0x9b05; state.sl[5] := 0x688c;
      state.sh[6] := 0x1f83; state.sl[6] := 0xd9ab;
      state.sh[7] := 0x5be0; state.sl[7] := 0xcd19;
    };
  };

  public func writeByte(state: StaticSha256, val: Nat8){
    if (state.high){
      state.word := nat8To16(val) << 8;
      state.high := false;
    } else {
      state.msg[Nat8.toNat(state.i_msg)] := state.word | nat8To16(val);
      state.i_msg +%= 1;
      state.high := true;
    };

    if (state.i_msg == 32) {
        process_block(state);
        state.i_msg := 0;
        state.i_block +%= 1;
      };
  };

  public func writePadding(state: StaticSha256): (){
    // n_bits = length of message in bits
    let t : Nat8 = if (state.high) state.i_msg << 1 else state.i_msg << 1 +% 1;
    let n_bits : Nat64 = ((nat32To64(state.i_block) << 6) +% Nat64.fromIntWrap(Nat8.toNat(t))) << 3;
    // separator byte
    if (state.high) {
      state.msg[Nat8.toNat(state.i_msg)] := 0x8000;
    } else {
      state.msg[Nat8.toNat(state.i_msg)] := state.word | 0x80;
    };
    state.i_msg +%= 1;
    // zero padding with extra block if necessary
    if (state.i_msg > 28) {
      while (state.i_msg < 32) {
        state.msg[Nat8.toNat(state.i_msg)] := 0;
        state.i_msg +%= 1;
      };
      process_block(state);
      state.i_msg := 0;
      // skipping here: state.i_block +%= 1;
    };
    // zero padding in last block
    while (state.i_msg < 28) {
      state.msg[Nat8.toNat(state.i_msg)] := 0;
      state.i_msg +%= 1;
    };
    // 8 length bytes
    // Note: this exactly fills the block buffer, hence process_block will get
    // triggered by the last writeByte
    let lh = nat64To32(n_bits >> 32);
    let ll = nat64To32(n_bits & 0xffffffff);
    state.msg[28] := nat32To16(lh >> 16);
    state.msg[29] := nat32To16(lh & 0xffff);
    state.msg[30] := nat32To16(ll >> 16);
    state.msg[31] := nat32To16(ll & 0xffff);
    process_block(state);
    // skipping here: state.i_msg := 0;
  };

  public func writeIter(state: StaticSha256, iter : { next() : ?Nat8 }) : () {
    label reading loop {
      switch (iter.next()) {
        case (?val) {
          writeByte(state, val);
          continue reading;
        };
        case (null) {
          break reading;
        };
      };
    };
  };

  public func writeArray(state: StaticSha256, arr : [Nat8]) : () = writeIter(state, arr.vals());
  public func writeBlob(state: StaticSha256, blob : Blob) : () = writeIter(state, blob.vals());

  public func sum(state: StaticSha256): Blob {
    writePadding(state);

    state.digest[0] := nat16To8(state.sh[0] >> 8);
    state.digest[1] := nat16To8(state.sh[0] & 0xff);
    state.digest[2] := nat16To8(state.sl[0] >> 8);
    state.digest[3] := nat16To8(state.sl[0] & 0xff);
    state.digest[4] := nat16To8(state.sh[1] >> 8);
    state.digest[5] := nat16To8(state.sh[1] & 0xff);
    state.digest[6] := nat16To8(state.sl[1] >> 8);
    state.digest[7] := nat16To8(state.sl[1] & 0xff);
    state.digest[8] := nat16To8(state.sh[2] >> 8);
    state.digest[9] := nat16To8(state.sh[2] & 0xff);
    state.digest[10] := nat16To8(state.sl[2] >> 8);
    state.digest[11] := nat16To8(state.sl[2] & 0xff);
    state.digest[12] := nat16To8(state.sh[3] >> 8);
    state.digest[13] := nat16To8(state.sh[3] & 0xff);
    state.digest[14] := nat16To8(state.sl[3] >> 8);
    state.digest[15] := nat16To8(state.sl[3] & 0xff);
    state.digest[16] := nat16To8(state.sh[4] >> 8);
    state.digest[17] := nat16To8(state.sh[4] & 0xff);
    state.digest[18] := nat16To8(state.sl[4] >> 8);
    state.digest[19] := nat16To8(state.sl[4] & 0xff);
    state.digest[20] := nat16To8(state.sh[5] >> 8);
    state.digest[21] := nat16To8(state.sh[5] & 0xff);
    state.digest[22] := nat16To8(state.sl[5] >> 8);
    state.digest[23] := nat16To8(state.sl[5] & 0xff);
    state.digest[24] := nat16To8(state.sh[6] >> 8);
    state.digest[25] := nat16To8(state.sh[6] & 0xff);
    state.digest[26] := nat16To8(state.sl[6] >> 8);
    state.digest[27] := nat16To8(state.sl[6] & 0xff);

    if (state.algo == #sha224) return Blob.fromArrayMut(state.digest);

    state.digest[28] := nat16To8(state.sh[7] >> 8);
    state.digest[29] := nat16To8(state.sh[7] & 0xff);
    state.digest[30] := nat16To8(state.sl[7] >> 8);
    state.digest[31] := nat16To8(state.sl[7] & 0xff);

    return Blob.fromArrayMut(state.digest);
  };

  func process_block(state: StaticSha256) {

      let w00 = nat16To32(state.msg[0]) << 16 | nat16To32(state.msg[1]);
      let w01 = nat16To32(state.msg[2]) << 16 | nat16To32(state.msg[3]);
      let w02 = nat16To32(state.msg[4]) << 16 | nat16To32(state.msg[5]);
      let w03 = nat16To32(state.msg[6]) << 16 | nat16To32(state.msg[7]);
      let w04 = nat16To32(state.msg[8]) << 16 | nat16To32(state.msg[9]);
      let w05 = nat16To32(state.msg[10]) << 16 | nat16To32(state.msg[11]);
      let w06 = nat16To32(state.msg[12]) << 16 | nat16To32(state.msg[13]);
      let w07 = nat16To32(state.msg[14]) << 16 | nat16To32(state.msg[15]);
      let w08 = nat16To32(state.msg[16]) << 16 | nat16To32(state.msg[17]);
      let w09 = nat16To32(state.msg[18]) << 16 | nat16To32(state.msg[19]);
      let w10 = nat16To32(state.msg[20]) << 16 | nat16To32(state.msg[21]);
      let w11 = nat16To32(state.msg[22]) << 16 | nat16To32(state.msg[23]);
      let w12 = nat16To32(state.msg[24]) << 16 | nat16To32(state.msg[25]);
      let w13 = nat16To32(state.msg[26]) << 16 | nat16To32(state.msg[27]);
      let w14 = nat16To32(state.msg[28]) << 16 | nat16To32(state.msg[29]);
      let w15 = nat16To32(state.msg[30]) << 16 | nat16To32(state.msg[31]);
      let w16 = w00 +% rot(w01, 07) ^ rot(w01, 18) ^ (w01 >> 03) +% w09 +% rot(w14, 17) ^ rot(w14, 19) ^ (w14 >> 10);
      let w17 = w01 +% rot(w02, 07) ^ rot(w02, 18) ^ (w02 >> 03) +% w10 +% rot(w15, 17) ^ rot(w15, 19) ^ (w15 >> 10);
      let w18 = w02 +% rot(w03, 07) ^ rot(w03, 18) ^ (w03 >> 03) +% w11 +% rot(w16, 17) ^ rot(w16, 19) ^ (w16 >> 10);
      let w19 = w03 +% rot(w04, 07) ^ rot(w04, 18) ^ (w04 >> 03) +% w12 +% rot(w17, 17) ^ rot(w17, 19) ^ (w17 >> 10);
      let w20 = w04 +% rot(w05, 07) ^ rot(w05, 18) ^ (w05 >> 03) +% w13 +% rot(w18, 17) ^ rot(w18, 19) ^ (w18 >> 10);
      let w21 = w05 +% rot(w06, 07) ^ rot(w06, 18) ^ (w06 >> 03) +% w14 +% rot(w19, 17) ^ rot(w19, 19) ^ (w19 >> 10);
      let w22 = w06 +% rot(w07, 07) ^ rot(w07, 18) ^ (w07 >> 03) +% w15 +% rot(w20, 17) ^ rot(w20, 19) ^ (w20 >> 10);
      let w23 = w07 +% rot(w08, 07) ^ rot(w08, 18) ^ (w08 >> 03) +% w16 +% rot(w21, 17) ^ rot(w21, 19) ^ (w21 >> 10);
      let w24 = w08 +% rot(w09, 07) ^ rot(w09, 18) ^ (w09 >> 03) +% w17 +% rot(w22, 17) ^ rot(w22, 19) ^ (w22 >> 10);
      let w25 = w09 +% rot(w10, 07) ^ rot(w10, 18) ^ (w10 >> 03) +% w18 +% rot(w23, 17) ^ rot(w23, 19) ^ (w23 >> 10);
      let w26 = w10 +% rot(w11, 07) ^ rot(w11, 18) ^ (w11 >> 03) +% w19 +% rot(w24, 17) ^ rot(w24, 19) ^ (w24 >> 10);
      let w27 = w11 +% rot(w12, 07) ^ rot(w12, 18) ^ (w12 >> 03) +% w20 +% rot(w25, 17) ^ rot(w25, 19) ^ (w25 >> 10);
      let w28 = w12 +% rot(w13, 07) ^ rot(w13, 18) ^ (w13 >> 03) +% w21 +% rot(w26, 17) ^ rot(w26, 19) ^ (w26 >> 10);
      let w29 = w13 +% rot(w14, 07) ^ rot(w14, 18) ^ (w14 >> 03) +% w22 +% rot(w27, 17) ^ rot(w27, 19) ^ (w27 >> 10);
      let w30 = w14 +% rot(w15, 07) ^ rot(w15, 18) ^ (w15 >> 03) +% w23 +% rot(w28, 17) ^ rot(w28, 19) ^ (w28 >> 10);
      let w31 = w15 +% rot(w16, 07) ^ rot(w16, 18) ^ (w16 >> 03) +% w24 +% rot(w29, 17) ^ rot(w29, 19) ^ (w29 >> 10);
      let w32 = w16 +% rot(w17, 07) ^ rot(w17, 18) ^ (w17 >> 03) +% w25 +% rot(w30, 17) ^ rot(w30, 19) ^ (w30 >> 10);
      let w33 = w17 +% rot(w18, 07) ^ rot(w18, 18) ^ (w18 >> 03) +% w26 +% rot(w31, 17) ^ rot(w31, 19) ^ (w31 >> 10);
      let w34 = w18 +% rot(w19, 07) ^ rot(w19, 18) ^ (w19 >> 03) +% w27 +% rot(w32, 17) ^ rot(w32, 19) ^ (w32 >> 10);
      let w35 = w19 +% rot(w20, 07) ^ rot(w20, 18) ^ (w20 >> 03) +% w28 +% rot(w33, 17) ^ rot(w33, 19) ^ (w33 >> 10);
      let w36 = w20 +% rot(w21, 07) ^ rot(w21, 18) ^ (w21 >> 03) +% w29 +% rot(w34, 17) ^ rot(w34, 19) ^ (w34 >> 10);
      let w37 = w21 +% rot(w22, 07) ^ rot(w22, 18) ^ (w22 >> 03) +% w30 +% rot(w35, 17) ^ rot(w35, 19) ^ (w35 >> 10);
      let w38 = w22 +% rot(w23, 07) ^ rot(w23, 18) ^ (w23 >> 03) +% w31 +% rot(w36, 17) ^ rot(w36, 19) ^ (w36 >> 10);
      let w39 = w23 +% rot(w24, 07) ^ rot(w24, 18) ^ (w24 >> 03) +% w32 +% rot(w37, 17) ^ rot(w37, 19) ^ (w37 >> 10);
      let w40 = w24 +% rot(w25, 07) ^ rot(w25, 18) ^ (w25 >> 03) +% w33 +% rot(w38, 17) ^ rot(w38, 19) ^ (w38 >> 10);
      let w41 = w25 +% rot(w26, 07) ^ rot(w26, 18) ^ (w26 >> 03) +% w34 +% rot(w39, 17) ^ rot(w39, 19) ^ (w39 >> 10);
      let w42 = w26 +% rot(w27, 07) ^ rot(w27, 18) ^ (w27 >> 03) +% w35 +% rot(w40, 17) ^ rot(w40, 19) ^ (w40 >> 10);
      let w43 = w27 +% rot(w28, 07) ^ rot(w28, 18) ^ (w28 >> 03) +% w36 +% rot(w41, 17) ^ rot(w41, 19) ^ (w41 >> 10);
      let w44 = w28 +% rot(w29, 07) ^ rot(w29, 18) ^ (w29 >> 03) +% w37 +% rot(w42, 17) ^ rot(w42, 19) ^ (w42 >> 10);
      let w45 = w29 +% rot(w30, 07) ^ rot(w30, 18) ^ (w30 >> 03) +% w38 +% rot(w43, 17) ^ rot(w43, 19) ^ (w43 >> 10);
      let w46 = w30 +% rot(w31, 07) ^ rot(w31, 18) ^ (w31 >> 03) +% w39 +% rot(w44, 17) ^ rot(w44, 19) ^ (w44 >> 10);
      let w47 = w31 +% rot(w32, 07) ^ rot(w32, 18) ^ (w32 >> 03) +% w40 +% rot(w45, 17) ^ rot(w45, 19) ^ (w45 >> 10);
      let w48 = w32 +% rot(w33, 07) ^ rot(w33, 18) ^ (w33 >> 03) +% w41 +% rot(w46, 17) ^ rot(w46, 19) ^ (w46 >> 10);
      let w49 = w33 +% rot(w34, 07) ^ rot(w34, 18) ^ (w34 >> 03) +% w42 +% rot(w47, 17) ^ rot(w47, 19) ^ (w47 >> 10);
      let w50 = w34 +% rot(w35, 07) ^ rot(w35, 18) ^ (w35 >> 03) +% w43 +% rot(w48, 17) ^ rot(w48, 19) ^ (w48 >> 10);
      let w51 = w35 +% rot(w36, 07) ^ rot(w36, 18) ^ (w36 >> 03) +% w44 +% rot(w49, 17) ^ rot(w49, 19) ^ (w49 >> 10);
      let w52 = w36 +% rot(w37, 07) ^ rot(w37, 18) ^ (w37 >> 03) +% w45 +% rot(w50, 17) ^ rot(w50, 19) ^ (w50 >> 10);
      let w53 = w37 +% rot(w38, 07) ^ rot(w38, 18) ^ (w38 >> 03) +% w46 +% rot(w51, 17) ^ rot(w51, 19) ^ (w51 >> 10);
      let w54 = w38 +% rot(w39, 07) ^ rot(w39, 18) ^ (w39 >> 03) +% w47 +% rot(w52, 17) ^ rot(w52, 19) ^ (w52 >> 10);
      let w55 = w39 +% rot(w40, 07) ^ rot(w40, 18) ^ (w40 >> 03) +% w48 +% rot(w53, 17) ^ rot(w53, 19) ^ (w53 >> 10);
      let w56 = w40 +% rot(w41, 07) ^ rot(w41, 18) ^ (w41 >> 03) +% w49 +% rot(w54, 17) ^ rot(w54, 19) ^ (w54 >> 10);
      let w57 = w41 +% rot(w42, 07) ^ rot(w42, 18) ^ (w42 >> 03) +% w50 +% rot(w55, 17) ^ rot(w55, 19) ^ (w55 >> 10);
      let w58 = w42 +% rot(w43, 07) ^ rot(w43, 18) ^ (w43 >> 03) +% w51 +% rot(w56, 17) ^ rot(w56, 19) ^ (w56 >> 10);
      let w59 = w43 +% rot(w44, 07) ^ rot(w44, 18) ^ (w44 >> 03) +% w52 +% rot(w57, 17) ^ rot(w57, 19) ^ (w57 >> 10);
      let w60 = w44 +% rot(w45, 07) ^ rot(w45, 18) ^ (w45 >> 03) +% w53 +% rot(w58, 17) ^ rot(w58, 19) ^ (w58 >> 10);
      let w61 = w45 +% rot(w46, 07) ^ rot(w46, 18) ^ (w46 >> 03) +% w54 +% rot(w59, 17) ^ rot(w59, 19) ^ (w59 >> 10);
      let w62 = w46 +% rot(w47, 07) ^ rot(w47, 18) ^ (w47 >> 03) +% w55 +% rot(w60, 17) ^ rot(w60, 19) ^ (w60 >> 10);
      let w63 = w47 +% rot(w48, 07) ^ rot(w48, 18) ^ (w48 >> 03) +% w56 +% rot(w61, 17) ^ rot(w61, 19) ^ (w61 >> 10);

      // for ((i, j, k, l, m) in expansion_rounds.vals()) {
      //   // (j,k,l,m) = (i+1,i+9,i+14,i+16)
      //   let (v0, v1) = (state.msg[j], state.msg[l]);
      //   let s0 = rot(v0, 07) ^ rot(v0, 18) ^ (v0 >> 03);
      //   let s1 = rot(v1, 17) ^ rot(v1, 19) ^ (v1 >> 10);
      //   state.msg[m] := msg[i] +% s0 +% msg[k] +% s1;
      // };
      
      // compress
      let a_0 = nat16To32(state.sh[0]) << 16 | nat16To32(state.sl[0]);
      let b_0 = nat16To32(state.sh[1]) << 16 | nat16To32(state.sl[1]);
      let c_0 = nat16To32(state.sh[2]) << 16 | nat16To32(state.sl[2]);
      let d_0 = nat16To32(state.sh[3]) << 16 | nat16To32(state.sl[3]);
      let e_0 = nat16To32(state.sh[4]) << 16 | nat16To32(state.sl[4]);
      let f_0 = nat16To32(state.sh[5]) << 16 | nat16To32(state.sl[5]);
      let g_0 = nat16To32(state.sh[6]) << 16 | nat16To32(state.sl[6]);
      let h_0 = nat16To32(state.sh[7]) << 16 | nat16To32(state.sl[7]);
      var a = a_0;
      var b = b_0;
      var c = c_0;
      var d = d_0;
      var e = e_0;
      var f = f_0;
      var g = g_0;
      var h = h_0;
      var t = 0 : Nat32;

      t := h +% K00 +% w00 +% (e & f) ^ (^ e & g) +% rot(e, 06) ^ rot(e, 11) ^ rot(e, 25); h := g; g := f; f := e; e := d +% t; d := c; c := b; b := a; a := t +% (b & c) ^ (b & d) ^ (c & d) +% rot(a, 02) ^ rot(a, 13) ^ rot(a, 22);
      t := h +% K01 +% w01 +% (e & f) ^ (^ e & g) +% rot(e, 06) ^ rot(e, 11) ^ rot(e, 25); h := g; g := f; f := e; e := d +% t; d := c; c := b; b := a; a := t +% (b & c) ^ (b & d) ^ (c & d) +% rot(a, 02) ^ rot(a, 13) ^ rot(a, 22);
      t := h +% K02 +% w02 +% (e & f) ^ (^ e & g) +% rot(e, 06) ^ rot(e, 11) ^ rot(e, 25); h := g; g := f; f := e; e := d +% t; d := c; c := b; b := a; a := t +% (b & c) ^ (b & d) ^ (c & d) +% rot(a, 02) ^ rot(a, 13) ^ rot(a, 22);
      t := h +% K03 +% w03 +% (e & f) ^ (^ e & g) +% rot(e, 06) ^ rot(e, 11) ^ rot(e, 25); h := g; g := f; f := e; e := d +% t; d := c; c := b; b := a; a := t +% (b & c) ^ (b & d) ^ (c & d) +% rot(a, 02) ^ rot(a, 13) ^ rot(a, 22);
      t := h +% K04 +% w04 +% (e & f) ^ (^ e & g) +% rot(e, 06) ^ rot(e, 11) ^ rot(e, 25); h := g; g := f; f := e; e := d +% t; d := c; c := b; b := a; a := t +% (b & c) ^ (b & d) ^ (c & d) +% rot(a, 02) ^ rot(a, 13) ^ rot(a, 22);
      t := h +% K05 +% w05 +% (e & f) ^ (^ e & g) +% rot(e, 06) ^ rot(e, 11) ^ rot(e, 25); h := g; g := f; f := e; e := d +% t; d := c; c := b; b := a; a := t +% (b & c) ^ (b & d) ^ (c & d) +% rot(a, 02) ^ rot(a, 13) ^ rot(a, 22);
      t := h +% K06 +% w06 +% (e & f) ^ (^ e & g) +% rot(e, 06) ^ rot(e, 11) ^ rot(e, 25); h := g; g := f; f := e; e := d +% t; d := c; c := b; b := a; a := t +% (b & c) ^ (b & d) ^ (c & d) +% rot(a, 02) ^ rot(a, 13) ^ rot(a, 22);
      t := h +% K07 +% w07 +% (e & f) ^ (^ e & g) +% rot(e, 06) ^ rot(e, 11) ^ rot(e, 25); h := g; g := f; f := e; e := d +% t; d := c; c := b; b := a; a := t +% (b & c) ^ (b & d) ^ (c & d) +% rot(a, 02) ^ rot(a, 13) ^ rot(a, 22);
      t := h +% K08 +% w08 +% (e & f) ^ (^ e & g) +% rot(e, 06) ^ rot(e, 11) ^ rot(e, 25); h := g; g := f; f := e; e := d +% t; d := c; c := b; b := a; a := t +% (b & c) ^ (b & d) ^ (c & d) +% rot(a, 02) ^ rot(a, 13) ^ rot(a, 22);
      t := h +% K09 +% w09 +% (e & f) ^ (^ e & g) +% rot(e, 06) ^ rot(e, 11) ^ rot(e, 25); h := g; g := f; f := e; e := d +% t; d := c; c := b; b := a; a := t +% (b & c) ^ (b & d) ^ (c & d) +% rot(a, 02) ^ rot(a, 13) ^ rot(a, 22);
      t := h +% K10 +% w10 +% (e & f) ^ (^ e & g) +% rot(e, 06) ^ rot(e, 11) ^ rot(e, 25); h := g; g := f; f := e; e := d +% t; d := c; c := b; b := a; a := t +% (b & c) ^ (b & d) ^ (c & d) +% rot(a, 02) ^ rot(a, 13) ^ rot(a, 22);
      t := h +% K11 +% w11 +% (e & f) ^ (^ e & g) +% rot(e, 06) ^ rot(e, 11) ^ rot(e, 25); h := g; g := f; f := e; e := d +% t; d := c; c := b; b := a; a := t +% (b & c) ^ (b & d) ^ (c & d) +% rot(a, 02) ^ rot(a, 13) ^ rot(a, 22);
      t := h +% K12 +% w12 +% (e & f) ^ (^ e & g) +% rot(e, 06) ^ rot(e, 11) ^ rot(e, 25); h := g; g := f; f := e; e := d +% t; d := c; c := b; b := a; a := t +% (b & c) ^ (b & d) ^ (c & d) +% rot(a, 02) ^ rot(a, 13) ^ rot(a, 22);
      t := h +% K13 +% w13 +% (e & f) ^ (^ e & g) +% rot(e, 06) ^ rot(e, 11) ^ rot(e, 25); h := g; g := f; f := e; e := d +% t; d := c; c := b; b := a; a := t +% (b & c) ^ (b & d) ^ (c & d) +% rot(a, 02) ^ rot(a, 13) ^ rot(a, 22);
      t := h +% K14 +% w14 +% (e & f) ^ (^ e & g) +% rot(e, 06) ^ rot(e, 11) ^ rot(e, 25); h := g; g := f; f := e; e := d +% t; d := c; c := b; b := a; a := t +% (b & c) ^ (b & d) ^ (c & d) +% rot(a, 02) ^ rot(a, 13) ^ rot(a, 22);
      t := h +% K15 +% w15 +% (e & f) ^ (^ e & g) +% rot(e, 06) ^ rot(e, 11) ^ rot(e, 25); h := g; g := f; f := e; e := d +% t; d := c; c := b; b := a; a := t +% (b & c) ^ (b & d) ^ (c & d) +% rot(a, 02) ^ rot(a, 13) ^ rot(a, 22);
      t := h +% K16 +% w16 +% (e & f) ^ (^ e & g) +% rot(e, 06) ^ rot(e, 11) ^ rot(e, 25); h := g; g := f; f := e; e := d +% t; d := c; c := b; b := a; a := t +% (b & c) ^ (b & d) ^ (c & d) +% rot(a, 02) ^ rot(a, 13) ^ rot(a, 22);
      t := h +% K17 +% w17 +% (e & f) ^ (^ e & g) +% rot(e, 06) ^ rot(e, 11) ^ rot(e, 25); h := g; g := f; f := e; e := d +% t; d := c; c := b; b := a; a := t +% (b & c) ^ (b & d) ^ (c & d) +% rot(a, 02) ^ rot(a, 13) ^ rot(a, 22);
      t := h +% K18 +% w18 +% (e & f) ^ (^ e & g) +% rot(e, 06) ^ rot(e, 11) ^ rot(e, 25); h := g; g := f; f := e; e := d +% t; d := c; c := b; b := a; a := t +% (b & c) ^ (b & d) ^ (c & d) +% rot(a, 02) ^ rot(a, 13) ^ rot(a, 22);
      t := h +% K19 +% w19 +% (e & f) ^ (^ e & g) +% rot(e, 06) ^ rot(e, 11) ^ rot(e, 25); h := g; g := f; f := e; e := d +% t; d := c; c := b; b := a; a := t +% (b & c) ^ (b & d) ^ (c & d) +% rot(a, 02) ^ rot(a, 13) ^ rot(a, 22);
      t := h +% K20 +% w20 +% (e & f) ^ (^ e & g) +% rot(e, 06) ^ rot(e, 11) ^ rot(e, 25); h := g; g := f; f := e; e := d +% t; d := c; c := b; b := a; a := t +% (b & c) ^ (b & d) ^ (c & d) +% rot(a, 02) ^ rot(a, 13) ^ rot(a, 22);
      t := h +% K21 +% w21 +% (e & f) ^ (^ e & g) +% rot(e, 06) ^ rot(e, 11) ^ rot(e, 25); h := g; g := f; f := e; e := d +% t; d := c; c := b; b := a; a := t +% (b & c) ^ (b & d) ^ (c & d) +% rot(a, 02) ^ rot(a, 13) ^ rot(a, 22);
      t := h +% K22 +% w22 +% (e & f) ^ (^ e & g) +% rot(e, 06) ^ rot(e, 11) ^ rot(e, 25); h := g; g := f; f := e; e := d +% t; d := c; c := b; b := a; a := t +% (b & c) ^ (b & d) ^ (c & d) +% rot(a, 02) ^ rot(a, 13) ^ rot(a, 22);
      t := h +% K23 +% w23 +% (e & f) ^ (^ e & g) +% rot(e, 06) ^ rot(e, 11) ^ rot(e, 25); h := g; g := f; f := e; e := d +% t; d := c; c := b; b := a; a := t +% (b & c) ^ (b & d) ^ (c & d) +% rot(a, 02) ^ rot(a, 13) ^ rot(a, 22);
      t := h +% K24 +% w24 +% (e & f) ^ (^ e & g) +% rot(e, 06) ^ rot(e, 11) ^ rot(e, 25); h := g; g := f; f := e; e := d +% t; d := c; c := b; b := a; a := t +% (b & c) ^ (b & d) ^ (c & d) +% rot(a, 02) ^ rot(a, 13) ^ rot(a, 22);
      t := h +% K25 +% w25 +% (e & f) ^ (^ e & g) +% rot(e, 06) ^ rot(e, 11) ^ rot(e, 25); h := g; g := f; f := e; e := d +% t; d := c; c := b; b := a; a := t +% (b & c) ^ (b & d) ^ (c & d) +% rot(a, 02) ^ rot(a, 13) ^ rot(a, 22);
      t := h +% K26 +% w26 +% (e & f) ^ (^ e & g) +% rot(e, 06) ^ rot(e, 11) ^ rot(e, 25); h := g; g := f; f := e; e := d +% t; d := c; c := b; b := a; a := t +% (b & c) ^ (b & d) ^ (c & d) +% rot(a, 02) ^ rot(a, 13) ^ rot(a, 22);
      t := h +% K27 +% w27 +% (e & f) ^ (^ e & g) +% rot(e, 06) ^ rot(e, 11) ^ rot(e, 25); h := g; g := f; f := e; e := d +% t; d := c; c := b; b := a; a := t +% (b & c) ^ (b & d) ^ (c & d) +% rot(a, 02) ^ rot(a, 13) ^ rot(a, 22);
      t := h +% K28 +% w28 +% (e & f) ^ (^ e & g) +% rot(e, 06) ^ rot(e, 11) ^ rot(e, 25); h := g; g := f; f := e; e := d +% t; d := c; c := b; b := a; a := t +% (b & c) ^ (b & d) ^ (c & d) +% rot(a, 02) ^ rot(a, 13) ^ rot(a, 22);
      t := h +% K29 +% w29 +% (e & f) ^ (^ e & g) +% rot(e, 06) ^ rot(e, 11) ^ rot(e, 25); h := g; g := f; f := e; e := d +% t; d := c; c := b; b := a; a := t +% (b & c) ^ (b & d) ^ (c & d) +% rot(a, 02) ^ rot(a, 13) ^ rot(a, 22);
      t := h +% K30 +% w30 +% (e & f) ^ (^ e & g) +% rot(e, 06) ^ rot(e, 11) ^ rot(e, 25); h := g; g := f; f := e; e := d +% t; d := c; c := b; b := a; a := t +% (b & c) ^ (b & d) ^ (c & d) +% rot(a, 02) ^ rot(a, 13) ^ rot(a, 22);
      t := h +% K31 +% w31 +% (e & f) ^ (^ e & g) +% rot(e, 06) ^ rot(e, 11) ^ rot(e, 25); h := g; g := f; f := e; e := d +% t; d := c; c := b; b := a; a := t +% (b & c) ^ (b & d) ^ (c & d) +% rot(a, 02) ^ rot(a, 13) ^ rot(a, 22);
      t := h +% K32 +% w32 +% (e & f) ^ (^ e & g) +% rot(e, 06) ^ rot(e, 11) ^ rot(e, 25); h := g; g := f; f := e; e := d +% t; d := c; c := b; b := a; a := t +% (b & c) ^ (b & d) ^ (c & d) +% rot(a, 02) ^ rot(a, 13) ^ rot(a, 22);
      t := h +% K33 +% w33 +% (e & f) ^ (^ e & g) +% rot(e, 06) ^ rot(e, 11) ^ rot(e, 25); h := g; g := f; f := e; e := d +% t; d := c; c := b; b := a; a := t +% (b & c) ^ (b & d) ^ (c & d) +% rot(a, 02) ^ rot(a, 13) ^ rot(a, 22);
      t := h +% K34 +% w34 +% (e & f) ^ (^ e & g) +% rot(e, 06) ^ rot(e, 11) ^ rot(e, 25); h := g; g := f; f := e; e := d +% t; d := c; c := b; b := a; a := t +% (b & c) ^ (b & d) ^ (c & d) +% rot(a, 02) ^ rot(a, 13) ^ rot(a, 22);
      t := h +% K35 +% w35 +% (e & f) ^ (^ e & g) +% rot(e, 06) ^ rot(e, 11) ^ rot(e, 25); h := g; g := f; f := e; e := d +% t; d := c; c := b; b := a; a := t +% (b & c) ^ (b & d) ^ (c & d) +% rot(a, 02) ^ rot(a, 13) ^ rot(a, 22);
      t := h +% K36 +% w36 +% (e & f) ^ (^ e & g) +% rot(e, 06) ^ rot(e, 11) ^ rot(e, 25); h := g; g := f; f := e; e := d +% t; d := c; c := b; b := a; a := t +% (b & c) ^ (b & d) ^ (c & d) +% rot(a, 02) ^ rot(a, 13) ^ rot(a, 22);
      t := h +% K37 +% w37 +% (e & f) ^ (^ e & g) +% rot(e, 06) ^ rot(e, 11) ^ rot(e, 25); h := g; g := f; f := e; e := d +% t; d := c; c := b; b := a; a := t +% (b & c) ^ (b & d) ^ (c & d) +% rot(a, 02) ^ rot(a, 13) ^ rot(a, 22);
      t := h +% K38 +% w38 +% (e & f) ^ (^ e & g) +% rot(e, 06) ^ rot(e, 11) ^ rot(e, 25); h := g; g := f; f := e; e := d +% t; d := c; c := b; b := a; a := t +% (b & c) ^ (b & d) ^ (c & d) +% rot(a, 02) ^ rot(a, 13) ^ rot(a, 22);
      t := h +% K39 +% w39 +% (e & f) ^ (^ e & g) +% rot(e, 06) ^ rot(e, 11) ^ rot(e, 25); h := g; g := f; f := e; e := d +% t; d := c; c := b; b := a; a := t +% (b & c) ^ (b & d) ^ (c & d) +% rot(a, 02) ^ rot(a, 13) ^ rot(a, 22);
      t := h +% K40 +% w40 +% (e & f) ^ (^ e & g) +% rot(e, 06) ^ rot(e, 11) ^ rot(e, 25); h := g; g := f; f := e; e := d +% t; d := c; c := b; b := a; a := t +% (b & c) ^ (b & d) ^ (c & d) +% rot(a, 02) ^ rot(a, 13) ^ rot(a, 22);
      t := h +% K41 +% w41 +% (e & f) ^ (^ e & g) +% rot(e, 06) ^ rot(e, 11) ^ rot(e, 25); h := g; g := f; f := e; e := d +% t; d := c; c := b; b := a; a := t +% (b & c) ^ (b & d) ^ (c & d) +% rot(a, 02) ^ rot(a, 13) ^ rot(a, 22);
      t := h +% K42 +% w42 +% (e & f) ^ (^ e & g) +% rot(e, 06) ^ rot(e, 11) ^ rot(e, 25); h := g; g := f; f := e; e := d +% t; d := c; c := b; b := a; a := t +% (b & c) ^ (b & d) ^ (c & d) +% rot(a, 02) ^ rot(a, 13) ^ rot(a, 22);
      t := h +% K43 +% w43 +% (e & f) ^ (^ e & g) +% rot(e, 06) ^ rot(e, 11) ^ rot(e, 25); h := g; g := f; f := e; e := d +% t; d := c; c := b; b := a; a := t +% (b & c) ^ (b & d) ^ (c & d) +% rot(a, 02) ^ rot(a, 13) ^ rot(a, 22);
      t := h +% K44 +% w44 +% (e & f) ^ (^ e & g) +% rot(e, 06) ^ rot(e, 11) ^ rot(e, 25); h := g; g := f; f := e; e := d +% t; d := c; c := b; b := a; a := t +% (b & c) ^ (b & d) ^ (c & d) +% rot(a, 02) ^ rot(a, 13) ^ rot(a, 22);
      t := h +% K45 +% w45 +% (e & f) ^ (^ e & g) +% rot(e, 06) ^ rot(e, 11) ^ rot(e, 25); h := g; g := f; f := e; e := d +% t; d := c; c := b; b := a; a := t +% (b & c) ^ (b & d) ^ (c & d) +% rot(a, 02) ^ rot(a, 13) ^ rot(a, 22);
      t := h +% K46 +% w46 +% (e & f) ^ (^ e & g) +% rot(e, 06) ^ rot(e, 11) ^ rot(e, 25); h := g; g := f; f := e; e := d +% t; d := c; c := b; b := a; a := t +% (b & c) ^ (b & d) ^ (c & d) +% rot(a, 02) ^ rot(a, 13) ^ rot(a, 22);
      t := h +% K47 +% w47 +% (e & f) ^ (^ e & g) +% rot(e, 06) ^ rot(e, 11) ^ rot(e, 25); h := g; g := f; f := e; e := d +% t; d := c; c := b; b := a; a := t +% (b & c) ^ (b & d) ^ (c & d) +% rot(a, 02) ^ rot(a, 13) ^ rot(a, 22);
      t := h +% K48 +% w48 +% (e & f) ^ (^ e & g) +% rot(e, 06) ^ rot(e, 11) ^ rot(e, 25); h := g; g := f; f := e; e := d +% t; d := c; c := b; b := a; a := t +% (b & c) ^ (b & d) ^ (c & d) +% rot(a, 02) ^ rot(a, 13) ^ rot(a, 22);
      t := h +% K49 +% w49 +% (e & f) ^ (^ e & g) +% rot(e, 06) ^ rot(e, 11) ^ rot(e, 25); h := g; g := f; f := e; e := d +% t; d := c; c := b; b := a; a := t +% (b & c) ^ (b & d) ^ (c & d) +% rot(a, 02) ^ rot(a, 13) ^ rot(a, 22);
      t := h +% K50 +% w50 +% (e & f) ^ (^ e & g) +% rot(e, 06) ^ rot(e, 11) ^ rot(e, 25); h := g; g := f; f := e; e := d +% t; d := c; c := b; b := a; a := t +% (b & c) ^ (b & d) ^ (c & d) +% rot(a, 02) ^ rot(a, 13) ^ rot(a, 22);
      t := h +% K51 +% w51 +% (e & f) ^ (^ e & g) +% rot(e, 06) ^ rot(e, 11) ^ rot(e, 25); h := g; g := f; f := e; e := d +% t; d := c; c := b; b := a; a := t +% (b & c) ^ (b & d) ^ (c & d) +% rot(a, 02) ^ rot(a, 13) ^ rot(a, 22);
      t := h +% K52 +% w52 +% (e & f) ^ (^ e & g) +% rot(e, 06) ^ rot(e, 11) ^ rot(e, 25); h := g; g := f; f := e; e := d +% t; d := c; c := b; b := a; a := t +% (b & c) ^ (b & d) ^ (c & d) +% rot(a, 02) ^ rot(a, 13) ^ rot(a, 22);
      t := h +% K53 +% w53 +% (e & f) ^ (^ e & g) +% rot(e, 06) ^ rot(e, 11) ^ rot(e, 25); h := g; g := f; f := e; e := d +% t; d := c; c := b; b := a; a := t +% (b & c) ^ (b & d) ^ (c & d) +% rot(a, 02) ^ rot(a, 13) ^ rot(a, 22);
      t := h +% K54 +% w54 +% (e & f) ^ (^ e & g) +% rot(e, 06) ^ rot(e, 11) ^ rot(e, 25); h := g; g := f; f := e; e := d +% t; d := c; c := b; b := a; a := t +% (b & c) ^ (b & d) ^ (c & d) +% rot(a, 02) ^ rot(a, 13) ^ rot(a, 22);
      t := h +% K55 +% w55 +% (e & f) ^ (^ e & g) +% rot(e, 06) ^ rot(e, 11) ^ rot(e, 25); h := g; g := f; f := e; e := d +% t; d := c; c := b; b := a; a := t +% (b & c) ^ (b & d) ^ (c & d) +% rot(a, 02) ^ rot(a, 13) ^ rot(a, 22);
      t := h +% K56 +% w56 +% (e & f) ^ (^ e & g) +% rot(e, 06) ^ rot(e, 11) ^ rot(e, 25); h := g; g := f; f := e; e := d +% t; d := c; c := b; b := a; a := t +% (b & c) ^ (b & d) ^ (c & d) +% rot(a, 02) ^ rot(a, 13) ^ rot(a, 22);
      t := h +% K57 +% w57 +% (e & f) ^ (^ e & g) +% rot(e, 06) ^ rot(e, 11) ^ rot(e, 25); h := g; g := f; f := e; e := d +% t; d := c; c := b; b := a; a := t +% (b & c) ^ (b & d) ^ (c & d) +% rot(a, 02) ^ rot(a, 13) ^ rot(a, 22);
      t := h +% K58 +% w58 +% (e & f) ^ (^ e & g) +% rot(e, 06) ^ rot(e, 11) ^ rot(e, 25); h := g; g := f; f := e; e := d +% t; d := c; c := b; b := a; a := t +% (b & c) ^ (b & d) ^ (c & d) +% rot(a, 02) ^ rot(a, 13) ^ rot(a, 22);
      t := h +% K59 +% w59 +% (e & f) ^ (^ e & g) +% rot(e, 06) ^ rot(e, 11) ^ rot(e, 25); h := g; g := f; f := e; e := d +% t; d := c; c := b; b := a; a := t +% (b & c) ^ (b & d) ^ (c & d) +% rot(a, 02) ^ rot(a, 13) ^ rot(a, 22);
      t := h +% K60 +% w60 +% (e & f) ^ (^ e & g) +% rot(e, 06) ^ rot(e, 11) ^ rot(e, 25); h := g; g := f; f := e; e := d +% t; d := c; c := b; b := a; a := t +% (b & c) ^ (b & d) ^ (c & d) +% rot(a, 02) ^ rot(a, 13) ^ rot(a, 22);
      t := h +% K61 +% w61 +% (e & f) ^ (^ e & g) +% rot(e, 06) ^ rot(e, 11) ^ rot(e, 25); h := g; g := f; f := e; e := d +% t; d := c; c := b; b := a; a := t +% (b & c) ^ (b & d) ^ (c & d) +% rot(a, 02) ^ rot(a, 13) ^ rot(a, 22);
      t := h +% K62 +% w62 +% (e & f) ^ (^ e & g) +% rot(e, 06) ^ rot(e, 11) ^ rot(e, 25); h := g; g := f; f := e; e := d +% t; d := c; c := b; b := a; a := t +% (b & c) ^ (b & d) ^ (c & d) +% rot(a, 02) ^ rot(a, 13) ^ rot(a, 22);
      t := h +% K63 +% w63 +% (e & f) ^ (^ e & g) +% rot(e, 06) ^ rot(e, 11) ^ rot(e, 25); h := g; g := f; f := e; e := d +% t; d := c; c := b; b := a; a := t +% (b & c) ^ (b & d) ^ (c & d) +% rot(a, 02) ^ rot(a, 13) ^ rot(a, 22);


      // for (i in compression_rounds.keys()) {
      //   let ch = (e & f) ^ (^ e & g);
      //   let maj = (a & b) ^ (a & c) ^ (b & c);
      //   let sigma0 = rot(a, 02) ^ rot(a, 13) ^ rot(a, 22);
      //   let sigma1 = rot(e, 06) ^ rot(e, 11) ^ rot(e, 25);
      //   let t = h +% K[i] +% state.msg[i] +% ch +% sigma1;
      //   h := g;
      //   g := f;
      //   f := e;
      //   e := d +% t;
      //   d := c;
      //   c := b;
      //   b := a;
      //   a := t +% maj +% sigma0;
      // };

      // final addition
      a +%= a_0;
      b +%= b_0;
      c +%= c_0;
      d +%= d_0;
      e +%= e_0;
      f +%= f_0;
      g +%= g_0;
      h +%= h_0;
      state.sh[0] := nat32To16(a >> 16); state.sl[0] := nat32To16(a & 0xffff);
      state.sh[1] := nat32To16(b >> 16); state.sl[1] := nat32To16(b & 0xffff);
      state.sh[2] := nat32To16(c >> 16); state.sl[2] := nat32To16(c & 0xffff);
      state.sh[3] := nat32To16(d >> 16); state.sl[3] := nat32To16(d & 0xffff);
      state.sh[4] := nat32To16(e >> 16); state.sl[4] := nat32To16(e & 0xffff);
      state.sh[5] := nat32To16(f >> 16); state.sl[5] := nat32To16(f & 0xffff);
      state.sh[6] := nat32To16(g >> 16); state.sl[6] := nat32To16(g & 0xffff);
      state.sh[7] := nat32To16(h >> 16); state.sl[7] := nat32To16(h & 0xffff);
    
  };

  public func fromBlob(algo : Algorithm, b : Blob) : Blob {
    let digest = Digest(algo);
    writeIter(digest, b.vals());
    return sum(digest);
  };

};
