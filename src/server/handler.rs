// Function Stream Service handler implementation

use protocol::service::{
    function_stream_service_server::FunctionStreamService, 
    LoginRequest, LogoutRequest, SqlRequest, Response, StatusCode,
};

use crate::sql::{Coordinator, SqlParser};
use std::sync::Arc;
use tokio::sync::RwLock;
use uuid::Uuid;

/// Session information
#[derive(Debug, Clone)]
pub struct Session {
    pub user_id: String,
    pub username: String,
    pub created_at: std::time::SystemTime,
}

pub struct FunctionStreamServiceImpl {
    sessions: Arc<RwLock<std::collections::HashMap<String, Session>>>,
    coordinator: Coordinator,
}

impl FunctionStreamServiceImpl {
    /// Create a new service instance
    pub fn new() -> Self {
        Self {
            sessions: Arc::new(RwLock::new(std::collections::HashMap::new())),
            coordinator: Coordinator::new(),
        }
    }

    /// Get session by user_id
    async fn get_session(&self, user_id: &str) -> Option<Session> {
        let sessions = self.sessions.read().await;
        sessions.get(user_id).cloned()
    }

    /// Create a new session
    async fn create_session(&self, username: String) -> Session {
        let user_id = Uuid::new_v4().to_string();
        let session = Session {
            user_id: user_id.clone(),
            username,
            created_at: std::time::SystemTime::now(),
        };

        let mut sessions = self.sessions.write().await;
        sessions.insert(user_id, session.clone());
        session
    }

    /// Remove session
    async fn remove_session(&self, user_id: &str) -> bool {
        let mut sessions = self.sessions.write().await;
        sessions.remove(user_id).is_some()
    }

    /// Clean up expired sessions (older than 24 hours)
    async fn cleanup_expired_sessions(&self) {
        let mut sessions = self.sessions.write().await;
        let now = std::time::SystemTime::now();
        let max_age = std::time::Duration::from_secs(24 * 60 * 60); // 24 hours

        sessions.retain(|_, session| {
            now.duration_since(session.created_at)
                .map(|age| age < max_age)
                .unwrap_or(false)
        });
    }
}

#[tonic::async_trait]
impl FunctionStreamService for FunctionStreamServiceImpl {
    /// Handle login request
    async fn login(
        &self,
        request: tonic::Request<LoginRequest>,
    ) -> Result<tonic::Response<Response>, tonic::Status> {
        let req = request.into_inner();
        log::info!("Received login request: username={}", req.username);

        // TODO: Implement actual authentication logic
        // Simple validation: username and password cannot be empty
        if req.username.is_empty() || req.password.is_empty() {
            return Ok(tonic::Response::new(Response {
                status_code: StatusCode::BadRequest as i32,
                message: "Username and password cannot be empty".to_string(),
                data: None,
            }));
        }

        // Simple authentication: username == password passes (should use database or auth service)
        if req.username == req.password {
            // Create session
            let session = self.create_session(req.username.clone()).await;
            log::info!("Login successful: user_id={}, username={}", session.user_id, session.username);

            Ok(tonic::Response::new(Response {
                status_code: StatusCode::Ok as i32,
                message: "Login successful".to_string(),
                data: Some(format!(
                    r#"{{"user_id":"{}","username":"{}"}}"#,
                    session.user_id, session.username
                )),
            }))
        } else {
            log::warn!("Login failed: username={}", req.username);
            Ok(tonic::Response::new(Response {
                status_code: StatusCode::Unauthorized as i32,
                message: "Invalid username or password".to_string(),
                data: None,
            }))
        }
    }

    /// Handle logout request
    async fn logout(
        &self,
        request: tonic::Request<LogoutRequest>,
    ) -> Result<tonic::Response<Response>, tonic::Status> {
        let _req = request.into_inner();
        log::info!("Received logout request");

        // TODO: Get user_id from request (can use metadata or token)
        // Simplified: cleanup all expired sessions
        self.cleanup_expired_sessions().await;

        log::info!("Logout successful");
        Ok(tonic::Response::new(Response {
            status_code: StatusCode::Ok as i32,
            message: "Logout successful".to_string(),
            data: None,
        }))
    }
    
    async fn execute_sql(
        &self,
        request: tonic::Request<SqlRequest>,
    ) -> Result<tonic::Response<Response>, tonic::Status> {
        // 记录整个SQL请求的开始时间（包括解析和执行）
        let start_time = std::time::Instant::now();
        let req = request.into_inner();
        log::info!("Received SQL request: {}", req.sql);
        
        // 解析阶段
        let parse_start = std::time::Instant::now();
        let stmt = match SqlParser::parse(&req.sql) {
            Ok(stmt) => {
                let parse_elapsed = parse_start.elapsed().as_secs_f64();
                log::info!("[Timing] SQL parsing: {:.3}s", parse_elapsed);
                stmt
            },
            Err(e) => {
                let parse_elapsed = parse_start.elapsed().as_secs_f64();
                let total_elapsed = start_time.elapsed().as_secs_f64();
                log::warn!("SQL parse error: {} (parse={:.3}s, total={:.3}s)", e, parse_elapsed, total_elapsed);
                return Ok(tonic::Response::new(Response {
                    status_code: StatusCode::BadRequest as i32,
                    message: format!("Parse error: {}", e),
                    data: None,
                }));
            }
        };
        
        // 协调器执行阶段
        let coord_start = std::time::Instant::now();
        let result = self.coordinator.execute(stmt.as_ref());
        let coord_elapsed = coord_start.elapsed().as_secs_f64();
        log::info!("[Timing] Coordinator execution: {:.3}s", coord_elapsed);
        
        // 计算总耗时（包括解析和执行）
        let elapsed = start_time.elapsed().as_secs_f64();
        log::info!("SQL request completed: total_elapsed={:.3}s", elapsed);
        
        let status_code = if result.success {
            StatusCode::Ok
        } else {
            StatusCode::InternalServerError
        };
        
        Ok(tonic::Response::new(Response {
            status_code: status_code as i32,
            message: result.message,
            data: result.data,
        }))
    }
}

impl Default for FunctionStreamServiceImpl {
    fn default() -> Self {
        Self::new()
    }
}

