use serde::{Deserialize, Serialize};

/// User Data Transfer Object for HTTP API
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct UserDTO {
    pub id: i32,
    pub username: String,
}
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GroupInfo {
    pub id: i64,
    pub name: String,
}

/// Messaggi inviati dal client al server
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ClientMessage {
    Register {
        username: String,
    },
    SendMessage {
        group_id: i64,
        content: String,
    },
    CreateGroup {
        name: String,
    },
    /// Invita un utente al gruppo (solo i membri possono invitare)
    InviteUser {
        group: String,
        username: String,
    },
    /// Accetta un invito pendente per un gruppo
    AcceptInvite {
        group: String,
    },
    /// Rifiuta un invito pendente per un gruppo
    RejectInvite {
        group: String,
    },
    /// Legacy: InviteToGroup con group_id numerico
    InviteToGroup {
        username_to_invite: String,
        group_id: i64,
    },
    /// Legacy: JoinGroup con group_id numerico (ora usa AcceptInvite)
    JoinGroup {
        group_id: i64,
    },
    ListGroups,
}

/// Messaggi inviati dal server al client
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ServerMessage {
    RegisterResponse {
        success: bool,
        reason: Option<String>,
    },
    Error {
        message: String,
    },
    NewMessage {
        group_id: i64,
        sender_username: String,
        content: String,
        timestamp: i64,
    },
    InviteReceived {
        group_id: i64,
        group_name: String,
        inviter_username: String,
    },
    UserJoinedGroup {
        group_id: i64,
        username: String,
    },
    GroupCreated {
        id: i64,
        name: String,
    },
    /// Conferma che un invito è stato inviato
    InviteSent {
        group: String,
        username: String,
    },
    /// Conferma che l'utente è stato aggiunto al gruppo
    JoinedGroup {
        group_id: i64,
        group_name: String,
    },
    /// Conferma che un invito è stato rifiutato
    InviteRejected {
        group: String,
    },
    GroupList {
        groups: Vec<GroupInfo>,
    },
}
