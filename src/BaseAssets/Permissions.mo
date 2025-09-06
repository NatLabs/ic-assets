import Array "mo:base@0/Array";
import IC "mo:ic@3";
import Set "mo:map@9/Set";
import T "Types";
import ErrorMessages "ErrorMessages";

/// This module handles permissions for the assets library.
/// It provides functions to grant, revoke, and check permissions of a caller's principal.
module {
  public type Permissions = {
    commit_principals : T.Set<Principal>;
    prepare_principals : T.Set<Principal>;
    manage_permissions_principals : T.Set<Principal>;
  };

  let { ic } = IC;

  /// Creates a new Permissions store.
  public func new() : Permissions {
    {
      commit_principals = Set.new();
      prepare_principals = Set.new();
      manage_permissions_principals = Set.new();
    };
  };

  /// Gets the permission set for a specific permission.
  public func get_permission_set(self : Permissions, permission : T.Permission) : T.Set<Principal> {
    switch (permission) {
      case (#Commit) self.commit_principals;
      case (#Prepare) self.prepare_principals;
      case (#Manage) self.manage_permissions_principals;
    };
  };

  /// Checks if a principal has a specific permission.
  public func has_permission(self : Permissions, principal : Principal, permission : T.Permission) : Bool {
    let permission_set = get_permission_set(self, permission);
    return Set.has(permission_set, T.phash, principal);
  };

  /// Checks if a principal has a specific permission and returns a result.
  public func has_permission_result(self : Permissions, principal : Principal, permission : T.Permission) : T.Result<(), Text> {
    if (has_permission(self, principal, permission)) return #ok();

    #err(ErrorMessages.missing_permission(debug_show permission));
  };

  /// Checks if a principal can perform a specific action.
  public func can_perform_action(self : Permissions, principal : Principal, action : T.Permission) : T.Result<(), Text> {
    var bool = has_permission(self, principal, action);
    bool := bool or (action == #Prepare and has_permission(self, principal, #Commit));

    if (bool) return #ok();
    #err(ErrorMessages.missing_permission(debug_show action));
  };

  /// Checks if a principal has the prepare permission.
  public func can_prepare(self : Permissions, principal : Principal) : T.Result<(), Text> {
    can_perform_action(self, principal, #Prepare);
  };

  /// Checks if a principal has the commit permission.
  public func can_commit(self : Permissions, principal : Principal) : T.Result<(), Text> {
    can_perform_action(self, principal, #Commit);
  };

  /// Gets the list of principals with a specific permission.
  public func get_permission_list(self : Permissions, permission : T.Permission) : [Principal] {
    let permission_set = get_permission_set(self, permission);
    return Set.toArray(permission_set);
  };

  /// Checks if a principal is a controller of the asset canister.
  public func is_controller(self : Permissions, canister_id : Principal, caller : Principal) : async* T.Result<(), Text> {

    let info = await ic.canister_info({
      canister_id;
      num_requested_changes = ?0;
    });

    let res = Array.find(info.controllers, func(p : Principal) : Bool = p == caller);
    switch (res) {
      case (?_) #ok();
      case (_) #err("Caller is not a controller.");
    };
  };

  /// Checks if a principal has the manage permissions permission.
  public func is_manager(self : Permissions, caller : Principal) : Bool {
    has_permission(self, caller, #Manage);
  };

  /// Checks if a principal is a manager or controller.
  public func is_manager_or_controller(self : Permissions, canister_id : Principal, caller : Principal) : async* T.Result<(), Text> {
    if (is_manager(self, caller)) return #ok();

    let #err(not_controller_msg) = await* is_controller(self, canister_id, caller) else return #ok();

    #err(ErrorMessages.missing_permission(debug_show #Manage) # " and " # not_controller_msg);
  };

  /// Grants a permission to a principal.
  public func grant_permission(self : Permissions, principal : Principal, permission : T.Permission) {
    let permission_set = get_permission_set(self, permission);
    ignore Set.put(permission_set, T.phash, principal);
  };

  /// Revokes a permission from a principal.
  public func revoke_permission(self : Permissions, principal : Principal, permission : T.Permission) {
    let permission_set = get_permission_set(self, permission);
    ignore Set.remove(permission_set, T.phash, principal);
  };

  /// Clears all permissions.
  public func clear_all(self : Permissions) {
    Set.clear(self.commit_principals);
    Set.clear(self.prepare_principals);
    Set.clear(self.manage_permissions_principals);
  };

  /// Clears a specific permission.
  public func clear(self : Permissions, permission : T.Permission) {
    let permission_set = get_permission_set(self, permission);
    Set.clear(permission_set);
  };

};
