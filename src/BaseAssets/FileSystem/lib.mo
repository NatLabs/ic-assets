import Text "mo:base@0/Text";
import Iter "mo:base@0/Iter";
import Buffer "mo:base@0/Buffer";
import Debug "mo:base@0/Debug";

import Itertools "mo:itertools@0/Iter";
import Map "mo:map@9/Map";
import CertifiedAssets "mo:certified-assets@0/Stable";
import RevIter "mo:itertools@0/RevIter";

import T "../Types";
import Common "Common";
import Asset "Asset";
import Certs "Certs";
import ErrorMessages "../ErrorMessages";

/// Virtual file system for storing assets in a hierarchical structure.
module {

  public type HierarchicalAssets = T.HierarchicalAssets;
  public type Directory = T.Map<Text, HierarchicalAssets>;
  public type DirectoryContent = T.DirectoryContent;

  public type FileSystem = {
    root : Directory;
    region : T.MemoryRegion;
    certs : T.CertifiedAssets;
  };

  public func new(region : T.MemoryRegion) : FileSystem {
    {
      root = Map.new();
      region;
      certs = CertifiedAssets.init_stable_store();
    };
  };

  func basename(path : Text) : Text {
    let parts = Iter.toArray(Text.split(path, #text("/")));
    parts[parts.size() - 1];
  };

  func dirname(path : Text) : Text {
    let parts = Iter.toArray(Text.split(path, #text("/")));
    Text.join("/", Itertools.take(parts.vals(), parts.size() - 1 : Nat));
  };

  public let {
    get_asset;
    get_html_alias_root;
    get_asset_using_aliases;
    get_fallback_asset;
    get_html_file_aliases;
  } = Common;

  public func get_directory(fs : FileSystem, key : T.Key) : T.Result<?T.Directory, Text> {

    let paths = Iter.toArray(Text.split(key, #text("/")));

    var map = fs.root;
    label extracting_asset for ((i, path) in Itertools.enumerate(paths.vals())) {
      if (path == "") if (i == 0 or i + 1 == paths.size()) {
        continue extracting_asset;
      } else {
        return #err("Invalid path: " # key);
      };

      map := switch (Map.get<Text, HierarchicalAssets>(map, T.thash, path)) {
        case (?#Directory(map)) map;
        case (?#Asset(asset)) {
          let asset_path = Text.join("/", Itertools.take(paths.vals(), i + 1));
          return #err("An asset exists at the path: " # asset_path);
        };
        case (null) return #ok(null);
      };

    };

    #ok(?map);

  };

  public func create_directory(fs : FileSystem, key : T.Key) : T.Result<T.Directory, Text> {

    let paths = Iter.toArray(Text.split(key, #text("/")));

    var map = fs.root;

    label creating_directory for ((i, path) in Itertools.enumerate(paths.vals())) {
      if (path == "") if (i == 0 or i + 1 == paths.size()) {
        continue creating_directory;
      } else {
        return #err("Invalid path: " # key);
      };

      map := switch (Map.get(map, T.thash, path)) {
        case (?#Asset(_)) return #err("Asset already exists with path: " # Text.join("/", Itertools.take(paths.vals(), i + 1)));
        case (?#Directory(directory_map)) directory_map;
        case (null) {
          let new_directory : T.Directory = Map.new();
          ignore Map.put(map, T.thash, path, #Directory(new_directory));
          new_directory;
        };
      };
    };

    #ok(map);

  };

  public func new_asset_record() : T.Asset = {
    encodings = Map.new();
    headers = Map.new();
    var content_type = "";
    var is_aliased = null;
    var max_age = null;
    var allow_raw_access = null;
    var last_certified_encoding = null;
  };

  public func create_asset(fs : FileSystem, key : T.Key) : T.Result<T.Asset, Text> {

    let asset_name = basename(key);
    let directory_name = dirname(key);

    let directory_map = switch (create_directory(fs, directory_name)) {
      case (#ok(directory_map)) directory_map;
      case (#err(msg)) return #err(msg);
    };

    switch (Map.get(directory_map, T.thash, asset_name)) {
      case (?#Asset(_)) #err("Asset already exists with path: " # key);
      case (?#Directory(_)) #err("Directory already exists with path: " # key);
      case (null) {
        let new_asset = Asset.new();
        ignore Map.put(directory_map, T.thash, asset_name, #Asset(new_asset));
        #ok(new_asset);
      };
    };
  };

  public func insert_asset(fs : FileSystem, key : T.Key, asset : T.Asset) : T.Result<?T.Asset, Text> {
    let directory_name = dirname(key);
    let asset_name = basename(key);

    let directory_map = switch (create_directory(fs, directory_name)) {
      case (#ok(directory_map)) directory_map;
      case (#err(msg)) return #err(msg);
    };

    let opt_prev = switch (Map.get(directory_map, T.thash, asset_name)) {
      case (?#Directory(_)) return #err("Directory already exists with path: " # key);
      case (?#Asset(prev)) {
        Certs.remove_asset_certificates(fs, key, prev, true);
        Asset.deallocate(fs, prev);
        ?prev;
      };
      case (null) null;
    };

    Certs.certify_asset(fs, key, asset, null);
    ignore Map.put(directory_map, T.thash, asset_name, #Asset(asset));

    #ok(opt_prev);
  };

  public func remove_asset(fs : FileSystem, key : T.Key) : T.Result<T.Asset, Text> {
    let directory_name = dirname(key);
    let asset_name = basename(key);

    let asset = switch (get_directory(fs, directory_name)) {
      case (#ok(?directory_map)) {
        switch (Map.get(directory_map, T.thash, asset_name)) {
          case (?#Asset(asset)) {
            ignore Map.remove(directory_map, T.thash, basename(key));
            asset;
          };
          case (?#Directory(_)) return #err("A directory exists at the path: " # key);
          case (null) return #err(ErrorMessages.asset_not_found(key));
        };
      };
      case (#ok(null)) return #err(ErrorMessages.asset_not_found(key));
      case (#err(msg)) return #err(msg);
    };

    Asset.deallocate(fs, asset);
    Certs.remove_asset_certificates(fs, key, asset, false);

    #ok(asset);

  };

  public func list_assets_in_directory(fs : FileSystem, key : T.Key) : T.Result<[T.Key], Text> {
    let directory_map = switch (get_directory(fs, key)) {
      case (#ok(?directory_map)) directory_map;
      case (#ok(null)) return #err("Directory not found: " # key);
      case (#err(msg)) return #err(msg);
    };

    let list = Iter.toArray(
      Itertools.mapFilter<(T.Key, T.HierarchicalAssets), T.Key>(
        Map.entries(directory_map),
        func(key : T.Key, value : HierarchicalAssets) : ?T.Key {
          switch (value) {
            case (#Asset(_)) ?key;
            case (#Directory(_)) null;
          };
        },
      )
    );

    #ok(list);
  };

  public func list_directories_in_directory(fs : FileSystem, key : T.Key) : T.Result<[T.Key], Text> {
    let directory_map = switch (get_directory(fs, key)) {
      case (#ok(?directory_map)) directory_map;
      case (#ok(null)) return #err("Directory not found: " # key);
      case (#err(msg)) return #err(msg);
    };

    let list = Iter.toArray(
      Itertools.mapFilter<(T.Key, T.HierarchicalAssets), T.Key>(
        Map.entries(directory_map),
        func(key : T.Key, value : HierarchicalAssets) : ?T.Key {
          switch (value) {
            case (#Asset(_)) null;
            case (#Directory(_)) ?key;
          };
        },
      )
    );

    #ok(list);
  };

  public func list_contents_in_directory(fs : FileSystem, key : T.Key) : T.Result<[DirectoryContent], Text> {
    let directory_map = switch (get_directory(fs, key)) {
      case (#ok(?directory_map)) directory_map;
      case (#ok(null)) return #err("Directory not found: " # key);
      case (#err(msg)) return #err(msg);
    };

    let list = Iter.toArray(
      Iter.map<(T.Key, T.HierarchicalAssets), DirectoryContent>(
        Map.entries(directory_map),
        func(key : T.Key, value : HierarchicalAssets) : DirectoryContent {
          switch (value) {
            case (#Asset(_)) ({ name = key; is_directory = false });
            case (#Directory(_)) ({
              name = key;
              is_directory = true;
            });
          };
        },
      )
    );

    #ok(list);
  };

  public func to_array(fs : FileSystem) : [(T.Key, T.Asset)] {

    let assets = Buffer.Buffer<(T.Key, T.Asset)>(8);

    let stack = Buffer.Buffer<(T.Key, T.Directory)>(8);
    stack.add(("", fs.root));

    while (stack.size() > 0) {
      let ?(path, map) = stack.removeLast() else Debug.trap("FileSystem.toArray(): stack should not be empty");

      for ((key, value) in Map.entries(map)) {
        let new_path = path # "/" # key;

        switch (value) {
          case (#Directory(map)) {
            stack.add((new_path, map));
          };
          case (#Asset(asset)) {
            assets.add((new_path, asset));
          };
        };
      };
    };

    Buffer.toArray(assets);

  };

  public func clear(fs : T.FileSystem) {
    //! do not clear region as it's shared with other modules
    // deallocating the assets should free up any memory used by the FileSystem module
    let assets = to_array(fs);

    for ((key, asset) in assets.vals()) {
      Asset.deallocate(fs, asset);
    };

    CertifiedAssets.clear(fs.certs);
    Map.clear(fs.root);

  };

  /// Merges the assets from `other` into `fs`
  /// Overwrites any existing assets with the same key
  /// Assumes that both FileSystems share the same MemoryRegion.

  public func merge(fs : T.FileSystem, other : T.FileSystem) {
    let other_assets = to_array(other);

    for ((key, asset) in other_assets.vals()) {
      switch (insert_asset(fs, key, asset)) {
        case (#ok(_)) {};
        case (#err(msg)) Debug.trap(msg);
      };
    };

  };

};
