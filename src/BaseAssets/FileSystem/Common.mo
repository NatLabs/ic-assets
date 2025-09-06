import Text "mo:base@0/Text";
import Iter "mo:base@0/Iter";
import Itertools "mo:itertools@0/Iter";
import Map "mo:map@9/Map";
import RevIter "mo:itertools@0/RevIter";

import T "../Types";

/// Common functions for the FileSystem module, used by the modules in the FileSystem directory.
/// Added in a separate module to avoid circular dependencies.
module {

  public func get_asset(fs : T.FileSystem, key : T.Key) : T.Result<?T.Asset, Text> {

    let paths = Iter.toArray(Text.split(key, #text("/")));

    var map = fs.root;

    label extracting_asset for ((i, path) in Itertools.enumerate(paths.vals())) {
      if (path == "") if (i == 0 or i + 1 == paths.size()) {
        continue extracting_asset;
      } else {
        return #err("Invalid path: " # key);
      };

      map := switch (Map.get<Text, T.HierarchicalAssets>(map, T.thash, path)) {
        case (?#Directory(map)) map;
        case (?#Asset(asset)) if (i + 1 == paths.size()) {
          return #ok(?asset);
        } else {
          let asset_path = Text.join("/", Itertools.take(paths.vals(), i + 1));
          return #err("Asset '" # asset_path # "' already exists at the provided path: " # key);
        };
        case (null) return #ok(null);
      };

    };

    #err("A directory exists at the provided path '" # key # "' instead of an asset");

  };

  public func get_html_alias_root(fs : T.FileSystem, alias : Text) : ?Text {
    if (not Text.endsWith(alias, #text ".html")) return ?(alias # "index.html");

    let ?sans_html = Text.stripEnd(alias, #text ".html") else return null;

    let ?sans_index = Text.stripEnd(sans_html, #text "/index") else return ?sans_html;

    return ?sans_index;
  };

  public func get_asset_using_aliases(fs : T.FileSystem, key : Text, using_aliases : Bool) : T.Result<?T.Asset, Text> {

    switch (get_asset(fs, key)) {
      case (#ok(?asset)) return #ok(?asset);
      case (#err(_)) {};
      case (#ok(null)) {};
    };

    if (not using_aliases) return #ok(null);

    let alias_root = switch (get_html_alias_root(fs, key)) {
      case (?key) key;
      case (_) return #ok(null);
    };

    switch (get_asset(fs, alias_root)) {
      case (#ok(?asset)) {
        if (asset.is_aliased != ?true) return #ok(null);
        return #ok(?asset);
      };
      case (#ok(null)) return #ok(null);
      case (#err(msg)) return #err(msg);
    };
  };

  public func join_paths(paths : [Text]) : Text {
    Text.join(
      "/",
      Itertools.flatten(
        Iter.map<Text, T.Iter<Text>>(
          paths.vals(),
          func(path : Text) : T.Iter<Text> {
            // assumes paths are formatted correctly
            // so we don't need to worry about double slashes
            Text.tokens(path, #text("/"));
          },
        )
      ),
    );
  };

  public func get_html_file_aliases(fs : T.FileSystem, key : Text) : [Text] {
    let aliases = if (Text.endsWith(key, #text("index.html"))) {
      // /index.html -> /
      [
        Text.trimEnd(key, #text("index.html"))
      ];
    } else if (Text.endsWith(key, #text ".html")) {
      return [];
    } else {
      // /test -> /test.html -> /test/index.html
      [
        join_paths([key, "index.html"]),
        key # ".html",
      ];
    };

    // an alias cannot overwrite an existing asset
    Iter.toArray(
      Iter.filter(
        aliases.vals(),
        func(alias : Text) : Bool {
          switch (get_asset(fs, alias)) {
            case (#ok(null) or #err(_)) true;
            case (#ok(?(_))) false;
          };
        },
      )
    )

  };

  public func get_fallback_asset(fs : T.FileSystem, key : Text) : ?(Text, T.Asset) {
    let paths = if (key == "/")[""] else Iter.toArray(Text.split(key, #text("/")));

    for (i in RevIter.range(0, paths.size()).rev()) {
      let slice = Itertools.fromArraySlice(paths, 0, i + 1);
      let possible_fallback_prefix = Text.join(("/"), slice);
      let possible_fallback_key = possible_fallback_prefix # "/index.html";

      switch (get_asset_using_aliases(fs, possible_fallback_key, true)) {
        case (#ok(?asset)) return ?(possible_fallback_key, asset);
        case (_) {};
      };
    };

    null;

  };
};
