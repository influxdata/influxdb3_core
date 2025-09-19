use std::{collections::HashMap, sync::Arc};

use arrow::datatypes::Schema;
use authz::{Authorization, Authorizer, http::AuthorizationHeaderExtension};
use bytes::Bytes;
use datafusion::{parquet::data_type::AsBytes, physical_plan::ExecutionPlan};
use futures::{StreamExt, stream::BoxStream};
use http::{
    HeaderValue, Method,
    header::{ACCEPT, CONTENT_TYPE},
    status::StatusCode,
};
use http_body_util::BodyExt;
use iox_http_util::{
    Request, Response, ResponseBuilder, empty_response_body, stream_bytes_to_response_body,
};
use iox_query::{
    QueryDatabase,
    exec::IOxSessionContext,
    query_log::{PermitAndToken, QueryCompletedToken, StatePlanned},
};
use iox_query_influxql::frontend::planner::InfluxQLQueryPlanner;
use iox_query_influxql_rewrite as rewrite;
use iox_query_params::StatementParams;
use mime::Mime;
use multer::Multipart;
use serde::Deserialize;
use serde_json::ser::{CompactFormatter, PrettyFormatter};
use trace::{TraceCollector, ctx::SpanContext, span::SpanExt};
use trace_http::{
    ctx::{RequestLogContext, RequestLogContextExt},
    query_variant::QueryVariant,
};
use tracing::{info, warn};

use super::{
    DEFAULT_CHUNK_SIZE, Error, QueryFormat, QueryParams, Result, StatementFuture, types::Precision,
};
use crate::{
    HttpError,
    response::{
        buffered::BufferedResponseStream,
        chunked::ChunkedResponseStream,
        csv::CsvStream,
        json::{BufferedJsonStream, ChunkedJsonStream},
        msgpack::{BufferedMessagePackStream, ChunkedMessagePackStream},
    },
    types::Statement,
};

#[derive(Debug)]
struct QueryPlan {
    physical_plan: Arc<dyn ExecutionPlan>,
    schema: Arc<Schema>,
    query_completed_token: QueryCompletedToken<StatePlanned>,
    context: IOxSessionContext,
}

#[derive(Debug, Clone)]
pub struct V1HttpHandler {
    database: Arc<dyn QueryDatabase>,
    authz: Option<Arc<dyn Authorizer>>,
    trace_collector: Option<Arc<dyn TraceCollector>>,
    iox_version: String,
}

impl V1HttpHandler {
    pub fn new(
        database: Arc<dyn QueryDatabase>,
        authz: Option<Arc<dyn Authorizer>>,
        trace_collector: Option<Arc<dyn TraceCollector>>,
        iox_version: String,
    ) -> Self {
        Self {
            database,
            authz,
            trace_collector,
            iox_version,
        }
    }

    pub async fn route_request(&self, req: Request) -> Result<Response, HttpError> {
        match (req.method(), req.uri().path()) {
            (&Method::GET | &Method::POST, "/query") => self
                .handle_parameterized_query(req)
                .await
                .inspect_err(|e| warn!("error encountered while handling /query: {:?}", e)),
            (&Method::GET | &Method::HEAD, "/ping") => self.ping(req).await,
            _ => Err(HttpError::NotFound(req.uri().path().to_owned())),
        }
    }

    async fn ping(&self, _req: Request) -> Result<Response, HttpError> {
        ResponseBuilder::new()
            .status(StatusCode::NO_CONTENT)
            // This is important for backwards compat with one of the clients
            .header("X-Influxdb-Build", "cloud2")
            .header("X-Influxdb-Version", self.iox_version.clone())
            .body(empty_response_body())
            .map_err(|e| HttpError::InternalError(e.to_string()))
    }

    async fn handle_parameterized_query(&self, mut req: Request) -> Result<Response, HttpError> {
        let span_ctx = Some(SpanContext::new_with_optional_collector(
            self.trace_collector.as_ref().map(Arc::clone),
        ));

        // Go ahead and get the token before we consume the body,
        // but we can't use it until later once we know the database.
        let token = self.get_token_from_request(&mut req)?;

        let (params, format) = extract_request(req).await?;

        let QueryParams {
            chunk_size,
            chunked,
            database,
            retention_policy,
            epoch,
            pretty: _,
            query,
            params,
        } = params;
        let chunk_size =
            chunked.and_then(|chunked| chunked.then(|| chunk_size.unwrap_or(DEFAULT_CHUNK_SIZE)));

        // Make a provided but empty db param None for better error messaging.
        let database = if let Some("") = database.as_deref() {
            None
        } else {
            database
        };

        if query.is_none() {
            return Err(HttpError::Invalid(
                "expected a query to be provided in the query string or body".to_owned(),
            ));
        }

        let query = query.unwrap();

        let sp: StatementParams = params
            .map(|s| serde_json::from_str(&s))
            .transpose()
            .map_err(Error::from)?
            .unwrap_or_default();

        let statements = rewrite::parse_statements(query.as_str()).map_err(Error::from);

        let statements = match statements {
            Ok(statements) => statements,
            Err(e) => {
                let statement = error_statement(e);
                let response = statements_to_response(vec![statement], chunk_size, epoch, format);

                return ResponseBuilder::new()
                    .status(200)
                    .header(CONTENT_TYPE, format.as_content_type())
                    .body(stream_bytes_to_response_body(response))
                    .map_err(|e| HttpError::InternalError(e.to_string()));
            }
        };

        let executing_statements = statements
            .into_iter()
            .map(|mut statement| {
                let fut = async {
                    // Handle retention policy
                    match (retention_policy.clone(), statement.retention_policy()) {
                        (None, None) | (None, Some(_)) => {}
                        (Some(rp), None) => {
                            statement.set_retention_policy(rp);
                        }
                        (Some(_), Some(_)) => {
                            // Influxqlbridge prioritizes the embedded dp/rp in the query
                            // over the params if both are specified.
                        }
                    };

                    // Resolve database
                    let database = match (database.clone(), statement.resolve_dbrp()) {
                        (None, None) => None,
                        (None, Some(db)) | (Some(db), None) => Some(db),
                        (Some(_), Some(q)) => {
                            // Influxqlbridge prioritizes the embedded dp/rp in the query
                            // over the params if both are specified.
                            Some(q)
                        }
                    };

                    let database = database.ok_or_else(|| Error::InfluxqlNoDatabase);
                    let database = match database {
                        Ok(database) => database,
                        Err(e) => return Ok::<_, Error>(error_statement(e)),
                    };

                    // Authorize request (this needs to be handled specially due to await)
                    // We'll return a Future here that will be awaited later
                    let token_clone = token.clone();
                    let sp_clone = sp.clone();
                    let span_ctx = span_ctx.clone();
                    // Authorize the request
                    let authz = self.authorize_request(token_clone, &database).await?;

                    // Generate the query
                    let query = statement.to_statement().to_string();

                    // Plan the query
                    let query_plan = self
                        .plan_query(
                            query,
                            database,
                            sp_clone,
                            authz.into_subject(),
                            span_ctx,
                            None,
                        )
                        .await;

                    match query_plan {
                        Ok(query_plan) => {
                            // Get executing statement
                            let query_statement_result_stream = get_executing_statement_from_plan(
                                query_plan,
                                Arc::clone(&self.database),
                            );

                            Ok(query_statement_result_stream)
                        }
                        Err(err) => Ok(error_statement(err)),
                    }
                };

                Ok::<_, Error>(fut)
            })
            .collect::<Result<Vec<_>, _>>()?;

        // Execute these futures
        let executing_statements = futures::future::try_join_all(executing_statements)
            .await?
            .into_iter()
            .collect::<Vec<_>>();

        let response = statements_to_response(executing_statements, chunk_size, epoch, format);

        ResponseBuilder::new()
            .status(200)
            .header(CONTENT_TYPE, format.as_content_type())
            .body(stream_bytes_to_response_body(response))
            .map_err(|e| HttpError::InternalError(e.to_string()))
    }

    async fn plan_query(
        &self,
        query: String,
        database: String,
        params: StatementParams,
        authz_id: Option<String>,
        span_ctx: Option<SpanContext>,
        external_span_ctx: Option<RequestLogContext>,
    ) -> Result<QueryPlan, Error> {
        let namespace: Arc<str> = database.into();
        let namespace_name = Arc::clone(&namespace);
        let namespace_name = namespace_name.as_ref();

        let db = self
            .database
            .namespace(namespace_name, span_ctx.child_span("get_namespace"), false)
            .await
            .map_err(Error::Database)?
            .ok_or(Error::DatabaseNotFound(namespace_name.to_string()))?;

        let query_completed_token = db.record_query(
            external_span_ctx.as_ref().map(RequestLogContext::ctx),
            QueryVariant::InfluxQl.str(),
            Box::new(query.to_string()),
            params.clone(),
            authz_id,
        );

        // Log after we acquire the permit and are about to start execution
        info!(
            %namespace_name,
            %query,
            trace=external_span_ctx.format_jaeger().as_str(),
            variant=QueryVariant::InfluxQl.str(),
            request_protocol="v1_http_query",
            "InfluxQL request planning",
        );

        let context = db.new_query_context(span_ctx, None);
        let planner_ctx = context.child_ctx("v1 query planner");
        // Run planner on a separate threadpool, rather than the IO pool that is servicing this request
        let physical_plan_res =
            context
                .run(async move {
                    InfluxQLQueryPlanner::query(query.as_ref(), params, &planner_ctx).await
                })
                .await;

        let (physical_plan, query_completed_token) = match physical_plan_res {
            Ok(physical_plan) => {
                let query_completed_token =
                    query_completed_token.planned(&context, Arc::clone(&physical_plan));
                (physical_plan, query_completed_token)
            }
            Err(e) => {
                query_completed_token.fail();
                Err(Error::from(e))?
            }
        };

        let schema = Arc::clone(&physical_plan.schema());
        Ok(QueryPlan {
            physical_plan,
            schema,
            query_completed_token,
            context,
        })
    }

    fn get_token_from_request(&self, req: &mut Request) -> Result<Option<Vec<u8>>, Error> {
        let token = if let Some(p) = extract_v1_auth_token(req) {
            Some(p)
        } else {
            let auth_header = req.extensions().get::<AuthorizationHeaderExtension>();
            auth_header
                .and_then(|auth_header| {
                    let header_value = &**auth_header;
                    header_value.as_ref().map(validate_auth_header)
                })
                .transpose()?
        };

        Ok(token)
    }

    async fn authorize_request(
        &self,
        token: Option<Vec<u8>>,
        database: &str,
    ) -> Result<Authorization, Error> {
        let required_permission = authz::Permission::ResourceAction(
            authz::Resource::Database(authz::Target::ResourceName(database.to_string())),
            authz::Action::Read,
        );

        self.authz
            .authorize(token, &[required_permission])
            .await
            .map_err(|e| Error::AuthorizationFailure(e.to_string()))
    }
}

fn statements_to_response(
    executing_statements: Vec<StatementFuture>,
    chunk_size: Option<usize>,
    epoch: Option<Precision>,
    format: QueryFormat,
) -> BoxStream<'static, Bytes> {
    match format {
        QueryFormat::Csv => CsvStream::new(executing_statements).boxed(),
        QueryFormat::Json => match chunk_size {
            Some(chunk_size) => {
                let response_stream = ChunkedResponseStream::new(executing_statements, chunk_size);
                ChunkedJsonStream::new(response_stream, || CompactFormatter, epoch).boxed()
            }
            None => {
                let response_stream = BufferedResponseStream::new(executing_statements);
                BufferedJsonStream::new(response_stream, || CompactFormatter, epoch).boxed()
            }
        },
        QueryFormat::JsonPretty => match chunk_size {
            Some(chunk_size) => {
                let response_stream = ChunkedResponseStream::new(executing_statements, chunk_size);
                ChunkedJsonStream::new(response_stream, PrettyFormatter::new, epoch).boxed()
            }
            None => {
                let response_stream = BufferedResponseStream::new(executing_statements);
                BufferedJsonStream::new(response_stream, PrettyFormatter::new, epoch).boxed()
            }
        },
        QueryFormat::MsgPack => match chunk_size {
            Some(chunk_size) => {
                let response_stream = ChunkedResponseStream::new(executing_statements, chunk_size);
                ChunkedMessagePackStream::new(response_stream, epoch).boxed()
            }
            None => {
                let response_stream = BufferedResponseStream::new(executing_statements);
                BufferedMessagePackStream::new(response_stream, epoch).boxed()
            }
        },
    }
}

fn get_executing_statement_from_plan(
    query_plan: QueryPlan,
    database: Arc<dyn QueryDatabase>,
) -> StatementFuture {
    let QueryPlan {
        physical_plan,
        schema,
        query_completed_token,
        context,
    } = query_plan;

    let fut = async move {
        let permit_span = context.child_span("query_rate_limit_semaphore");
        let permit = database.acquire_semaphore(permit_span).await;
        let query_completed_token: iox_query::query_log::QueryCompletedToken<
            iox_query::query_log::StatePermit,
        > = query_completed_token.permit();

        context
            .execute_stream(physical_plan)
            .await
            .map(|stream| {
                Statement::new(
                    Arc::clone(&schema),
                    PermitAndToken {
                        permit,
                        query_completed_token,
                    },
                    stream,
                )
            })
            .map_err(Error::from)
    };

    Box::new(fut)
}

fn error_statement(error: Error) -> StatementFuture {
    Box::new(futures::future::err(error))
}

#[derive(Debug, Deserialize)]
struct V1AuthParameters {
    #[serde(rename = "p")]
    password: Option<String>,
}

fn extract_v1_auth_token(req: &mut Request) -> Option<Vec<u8>> {
    req.uri()
        .path_and_query()
        .and_then(|pq| match pq.path() {
            "/query" => pq.query(),
            _ => None,
        })
        .map(serde_urlencoded::from_str::<V1AuthParameters>)
        .transpose()
        .ok()
        .flatten()
        .and_then(|params| params.password)
        .map(String::into_bytes)
}

fn validate_auth_header(header: &HeaderValue) -> Result<Vec<u8>> {
    let header = header.to_str().map_err(|e| Error::Utf8 {
        message: "auth header",
        error: e.to_string(),
    })?;
    authz::extract_token(Some(header)).ok_or(Error::AuthorizationFailure(
        "failed to extract token from header".to_owned(),
    ))
}

enum SupportedContentType {
    ApplicationInfluxql,
    FormUrlEncoded,
    MultipartFormData,
}

impl SupportedContentType {
    fn from_request(req: &Request) -> Result<Self> {
        if let Some(ct) = req.headers().get("Content-Type") {
            let ct = std::str::from_utf8(ct.as_bytes()).map_err(|e| Error::Utf8 {
                message: "mime type",
                error: e.to_string(),
            })?;
            let mime: Mime = ct
                .parse()
                .map_err(|x: mime::FromStrError| Error::InvalidMimeType(x.to_string()))?;

            match (mime.type_(), mime.subtype()) {
                (mime::APPLICATION, mime::WWW_FORM_URLENCODED) => Ok(Self::FormUrlEncoded),
                (mime::APPLICATION, subtype) if subtype.as_str() == "vnd.influxql" => {
                    Ok(Self::ApplicationInfluxql)
                }
                (mime::MULTIPART, mime::FORM_DATA) => Ok(Self::MultipartFormData),
                _ => Err(Error::InvalidMimeType(mime.to_string())),
            }
        } else {
            // Default to assuming an influxql POST body
            Ok(Self::ApplicationInfluxql)
        }
    }
}

async fn influxql_body(req: Request) -> Result<QueryParams, HttpError> {
    let mut params = QueryParams::from_request_query_string(&req)?;
    // We support a "q" query string for POST too.
    // If empty, check the content-type and parse the body appropriately.
    if params.query.as_ref().is_none_or(|x| x.is_empty()) {
        let bytes = req
            .into_body()
            .collect()
            .await
            .map_err(|_| {
                HttpError::Invalid("Error retrieving bytes from response body".to_owned())
            })?
            .to_bytes();
        params.query = Some(String::from_utf8(bytes.to_vec()).map_err(|_| {
            HttpError::Invalid("Error retrieving query from request body".to_owned())
        })?);
    };

    Ok(params)
}

async fn form_urlencoded(req: Request) -> Result<QueryParams, HttpError> {
    let (body_params, _) = form_urlencoded_inner(req).await?;
    Ok(body_params)
}

async fn form_urlencoded_inner(req: Request) -> Result<(QueryParams, Bytes), HttpError> {
    // The 1.x implementation uses [FormValue](https://pkg.go.dev/net/http#Request.FormValue)
    // which relies on [ParseForm](https://pkg.go.dev/net/http#Request.ParseForm).
    //
    // This will always parse the URL query string as well as parsing the form body when required.
    // Request body parameters take precedence over URL query string values.

    // It is okay to swallow the error here, since a query string is not mandatory.
    let query_string_params = QueryParams::from_request_query_string(&req).unwrap_or_default();

    let bytes = req
        .into_body()
        .collect()
        .await
        .map_err(|e| Error::FieldRead {
            name: "body",
            error: e.to_string(),
        })?
        .to_bytes();
    let mut body_params = QueryParams::from_bytes_form_urlencoded(&bytes)?;

    body_params.merge(query_string_params);

    Ok((body_params, bytes))
}

async fn multipart_upload(req: Request) -> Result<QueryParams, HttpError> {
    let boundary = req
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|ct| ct.to_str().ok())
        .and_then(|ct| multer::parse_boundary(ct).ok());

    if boundary.is_none() {
        return Err(HttpError::Invalid(
            "A boundary header is required for multipart upload".to_owned(),
        ));
    }

    let (lower_precedence_params, bytes) = form_urlencoded_inner(req).await?;

    let mut fields: HashMap<String, String> = HashMap::new();
    // all the fields are strings,
    // then parse in a method on QueryParams
    let mut multipart = Multipart::new(
        futures::stream::once(async { Ok::<_, Error>(bytes) }),
        boundary.unwrap(), // safe due to is_none check above
    );
    while let Some(mut field) = multipart
        .next_field()
        .await
        .map_err(|e| Error::MultipartFile(e.to_string()))?
    {
        if let Some(name) = field.name() {
            let name = name.to_owned();
            let mut value = Vec::new();
            while let Some(field_chunk) = field
                .chunk()
                .await
                .map_err(|e| Error::MultipartFile(e.to_string()))?
            {
                value.extend_from_slice(field_chunk.as_bytes());
            }

            let value = String::from_utf8(value).map_err(|e| Error::Utf8 {
                message: "multipart field",
                error: e.to_string(),
            })?;
            fields.insert(name, value);
        }
    }

    let mut body_params = QueryParams::from_hashmap_multipart(fields)?;
    body_params.merge(lower_precedence_params);

    Ok(body_params)
}

async fn extract_request(req: Request) -> Result<(QueryParams, QueryFormat), HttpError> {
    // Pull the mime_type out before we consume the body in the match
    let accept = req.headers().get(ACCEPT).cloned();
    let mime_type = accept.as_ref().map(HeaderValue::as_bytes);

    let qp = match *req.method() {
        Method::GET => Ok(QueryParams::from_request_query_string(&req)?),
        Method::POST => {
            let content_type = SupportedContentType::from_request(&req)?;
            match content_type {
                SupportedContentType::ApplicationInfluxql => influxql_body(req).await,
                SupportedContentType::FormUrlEncoded => form_urlencoded(req).await,
                SupportedContentType::MultipartFormData => multipart_upload(req).await,
            }
        }
        _ => Err(HttpError::Invalid("Invalid request method".to_owned())),
    }?;

    let qf = QueryFormat::from_bytes(mime_type, qp.pretty.unwrap_or_default())?;
    Ok((qp, qf))
}
