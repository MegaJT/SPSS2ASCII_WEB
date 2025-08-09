import dash
from dash import html, dcc, Output, Input, State, callback_context
import dash_bootstrap_components as dbc
import base64
import io
import os
import uuid
import tempfile
import shutil
import zipfile
from datetime import datetime
import threading
import queue
import logging
import json
from SPSStoASCII_PROCESS import process_spss_with_map, process_spss_wo_map, setup_logging, close_logging

# Configuration settings
CONFIG = {
    # File size limits in MB (set to None to disable size checking)
    'MAX_SPSS_FILE_SIZE_MB': 20,  # Maximum SPSS file size in MB
    'MAX_MAP_FILE_SIZE_MB': 5,    # Maximum Map file size in MB
    
    # To disable file size checking completely, set both to None:
    # 'MAX_SPSS_FILE_SIZE_MB': None,
    # 'MAX_MAP_FILE_SIZE_MB': None,
}

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP], assets_folder='../assets')
server = app.server

# Global variables for session management
sessions = {}
processing_status = {}

# Create temp directory for sessions
TEMP_DIR = os.path.join(os.getcwd(), "temp")
os.makedirs(TEMP_DIR, exist_ok=True)

# File counter management
COUNTER_FILE = os.path.join(TEMP_DIR, "conversion_counter.json")

def load_conversion_counter():
    """Load the conversion counter from file"""
    try:
        if os.path.exists(COUNTER_FILE):
            with open(COUNTER_FILE, 'r') as f:
                data = json.load(f)
                return data.get('total_conversions', 0)
    except Exception as e:
        print(f"Error loading counter: {e}")
    return 0

def save_conversion_counter(count):
    """Save the conversion counter to file"""
    try:
        data = {'total_conversions': count, 'last_updated': datetime.now().isoformat()}
        with open(COUNTER_FILE, 'w') as f:
            json.dump(data, f)
    except Exception as e:
        print(f"Error saving counter: {e}")

def increment_conversion_counter():
    """Increment and save the conversion counter"""
    current_count = load_conversion_counter()
    new_count = current_count + 1
    save_conversion_counter(new_count)
    return new_count

# Load initial counter
initial_counter = load_conversion_counter()

app.layout = dbc.Container([
    # Store components for session management
    dcc.Store(id="session-id", data=str(uuid.uuid4())),
    dcc.Store(id="spss-file-data"),
    dcc.Store(id="map-file-data"),
    dcc.Store(id="conversion-counter", data=initial_counter),
    dcc.Store(id="error-map-data"),  # Store for error map file path
    dcc.Interval(id="progress-interval", interval=1000, n_intervals=0, disabled=True),
    dcc.Download(id="download-zip"),
    dcc.Download(id="download-template"),
    dcc.Download(id="download-error-map"),  # New download for error map
    
    # Header (simplified without counter badge)
    dbc.Row([
        dbc.Col([
            html.H2("SPSS to Fixed-Width ASCII Converter", className="text-center mb-4")
        ], width=12)
    ], className="mb-4"),

    # Status Alert
    dbc.Row([
        dbc.Col([
            dbc.Alert(id="status-alert", is_open=False, dismissable=True, className="mb-3")
        ], width=10)
    ], justify="center"),

    # Upload SPSS File
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader("Step 1: Upload SPSS File"),
                dbc.CardBody([
                    dbc.Label("Upload SPSS File (.sav)", className="fw-bold"),
                    html.Small(
                        f"Maximum file size: {CONFIG['MAX_SPSS_FILE_SIZE_MB']} MB" if CONFIG['MAX_SPSS_FILE_SIZE_MB'] else "No file size limit",
                        className="text-muted mb-2 d-block"
                    ),
                    dcc.Upload(
                        id="upload-spss",
                        children=html.Div([
                            "Drag and Drop or ", html.A("Select SPSS File")
                        ]),
                        style={
                            "width": "100%", "height": "60px", "lineHeight": "60px",
                            "borderWidth": "1px", "borderStyle": "dashed",
                            "borderRadius": "5px", "textAlign": "center"
                        },
                        className="upload-area",
                        multiple=False
                    ),
                    html.Div(id="spss-upload-status", className="mt-2")
                ])
            ])
        ], width=8)
    ], justify="center", className="mb-4"),

    # Map File Toggle and Upload
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader("Step 2: Map File Configuration"),
                dbc.CardBody([
                    dbc.Checklist(
                        options=[{"label": "Use Existing Map File", "value": "use_map"}],
                        value=[],
                        id="toggle-map",
                        switch=True,
                        className="mb-3"
                    ),
                    html.Div(id="map-upload-section", children=[
                        dbc.Label("Upload Map File (.xlsx)", className="fw-bold"),
                        html.Small(
                            f"Maximum file size: {CONFIG['MAX_MAP_FILE_SIZE_MB']} MB" if CONFIG['MAX_MAP_FILE_SIZE_MB'] else "No file size limit",
                            className="text-muted mb-2 d-block"
                        ),
                        dcc.Upload(
                            id="upload-map",
                            children=html.Div([
                                "Drag and Drop or ", html.A("Select Map File")
                            ]),
                            style={
                                "width": "100%", "height": "60px", "lineHeight": "60px",
                                "borderWidth": "1px", "borderStyle": "dashed",
                                "borderRadius": "5px", "textAlign": "center"
                            },
                            className="upload-area",
                            multiple=False
                        ),
                        html.Div(id="map-upload-status", className="mt-2"),
                        html.Hr(),
                        dbc.Button("Download Map Template", id="download-template-btn", color="info", size="sm")
                    ], style={"display": "none"})
                ])
            ])
        ], width=8)
    ], justify="center", className="mb-4"),

    # Configuration Options
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader("Step 3: Processing Configuration"),
                dbc.CardBody([
                    dbc.Row([
                        dbc.Col([
                            dbc.Label("Zero Padding", className="fw-bold"),
                            dbc.Checklist(
                                options=[{"label": "Enable Zero Padding", "value": "zero_pad"}],
                                value=[],
                                id="zero-padding-toggle",
                                switch=True
                            ),
                            html.Small("Pads numeric values with leading zeros", className="text-muted")
                        ], width=6),
                        dbc.Col([
                            dbc.Label("Width Value", className="fw-bold"),
                            dbc.InputGroup([
                                dbc.Input(
                                    id="width-value-input",
                                    type="number",
                                    value=0,
                                    min=0,
                                    max=10,
                                    step=1
                                ),
                                dbc.InputGroupText("columns")
                            ]),
                            html.Small("Additional width padding (0-10)", className="text-muted")
                        ], width=6)
                    ])
                ])
            ])
        ], width=8)
    ], justify="center", className="mb-4"),

    # Processing Section
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader("Step 4: Process and Download"),
                dbc.CardBody([
                    dbc.Row([
                        dbc.Col([
                            dbc.Button(
                                "Convert to ASCII", 
                                id="convert-btn", 
                                color="primary", 
                                size="lg",
                                disabled=True,
                                className="me-3"
                            ),
                            dbc.Button(
                                "Download Results", 
                                id="download-results-btn", 
                                color="success",
                                size="lg",
                                disabled=True,
                                style={"display": "none"}
                            ),
                            # New button for downloading error map
                            dbc.Button(
                                "Download Error Map", 
                                id="download-error-map-btn", 
                                color="warning",
                                size="lg",
                                disabled=True,
                                style={"display": "none"},
                                className="ms-2"
                            )
                        ], width=12, className="text-center mb-3")
                    ]),
                    # Progress bar and spinner
                    html.Div(id="progress-section", style={"display": "none"}, children=[
                        dbc.Row([
                            dbc.Col([
                                dbc.Spinner(
                                    html.Div(id="spinner-content"),
                                    size="lg",
                                    color="primary",
                                    type="border",
                                    spinner_style={"width": "3rem", "height": "3rem"}
                                )
                            ], width=2, className="text-center"),
                            dbc.Col([
                                html.H6("Processing...", className="fw-bold mb-2"),
                                dbc.Progress(id="progress-bar", value=0, striped=True, animated=True)
                            ], width=10)
                        ], className="mb-3")
                    ]),
                    html.Div(id="processing-log", style={"display": "none"}, children=[
                        html.H6("Processing Log:", className="fw-bold"),
                        html.Div(id="log-content", style={
                            "height": "200px",
                            "overflow-y": "scroll",
                            "background-color": "#f8f9fa",
                            "padding": "10px",
                            "border": "1px solid #dee2e6",
                            "border-radius": "5px",
                            "font-family": "monospace",
                            "font-size": "12px",
                            "white-space": "pre-wrap"
                        })
                    ])
                ])
            ])
        ], width=8)
    ], justify="center", className="mb-4"),

    # Instructions with stats
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    dbc.Row([
                        dbc.Col([
                            html.H6("Instructions:", className="fw-bold"),
                            html.Ul([
                                html.Li("Upload your SPSS (.sav) file"),
                                html.Li("Optionally upload a map file or let the system auto-generate one"),
                                html.Li("Configure zero padding and width settings as needed"),
                                html.Li("Click 'Convert to ASCII' to process your file"),
                                html.Li("Download the results ZIP file (contains ASCII, MAP, and Log files)"),
                                html.Li("If map validation fails, download the error map to see issues"),
                                html.Li("For any issues, contact the developer via email")  
                            ]),
                            html.P([
                                html.Strong("Note: "), 
                                "This conversion does not convert string variables."
                            ], className="text-muted mb-0")
                        ], width=9),
                        dbc.Col([
                            html.Div([
                                html.H6("Statistics", className="fw-bold text-center"),
                                html.Hr(),
                                html.Div([
                                    html.I(className="fas fa-file-alt fa-2x text-primary mb-2"),
                                    html.H4(id="counter-display", children=f"{initial_counter:,}", className="text-primary mb-1"),
                                    html.Small("Files Converted", className="text-muted")
                                ], className="text-center")
                            ], className="border rounded p-3 bg-light")
                        ], width=3)
                    ])
                ])
            ])
        ], width=8)
    ], justify="center", className="mb-4"),
    
    # Beautiful Footer
    dbc.Row([
        dbc.Col([
            html.Div([
                html.Hr(style={"margin": "2rem 0 1rem 0", "opacity": "0.3"}),
                html.Div([
                    html.Span("Built with ", style={"color": "#6c757d"}),
                    html.Span("passion", style={"color": "#e74c3c", "font-weight": "bold"}),
                    html.Span(" by ", style={"color": "#6c757d"}),
                    html.Span("Jijo", style={"color": "#3498db", "font-weight": "bold"}),
                    html.Span(" | ", style={"color": "#6c757d", "margin": "0 8px"}),
                    html.I(className="fab fa-python", style={"color": "#3776ab", "margin": "0 4px"}),
                    html.Span("Python", style={"color": "#3776ab", "font-weight": "500"}),
                    html.Span(" + ", style={"color": "#6c757d", "margin": "0 4px"}),
                    html.Span("Dash", style={"color": "#119dff", "font-weight": "500"}),
                    html.Span(" + ", style={"color": "#6c757d", "margin": "0 4px"}),
                    html.I(className="fas fa-heart", style={"color": "#e74c3c", "margin": "0 4px"}),
                    html.Span("Love for Data Science", style={"color": "#2ecc71", "font-weight": "500"})
                ], className="text-center", style={
                    "font-size": "14px",
                    "padding": "1rem",
                    "background": "linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%)",
                    "border-radius": "10px",
                    "box-shadow": "0 2px 4px rgba(0,0,0,0.1)",
                    "margin": "0 auto",
                    "max-width": "500px",
                    "margin-bottom": "1rem"
                }),
                # Feedback Badge
                html.Div([
                    html.I(className="fas fa-comment-alt", style={"color": "#17a2b8", "margin-right": "8px"}),
                    html.Span("Feedback & Suggestions: ", style={"color": "#495057", "font-weight": "500"}),
                    html.A(
                        "jijo.thankachan@gmail.com",
                        href="mailto:jijo.thankachan@gmail.com?subject=SPSS Converter - Feedback",
                        style={
                            "color": "#007bff",
                            "text-decoration": "none",
                            "font-weight": "500",
                            "border-bottom": "1px dotted #007bff"
                        }
                    )
                ], className="text-center", style={
                    "font-size": "13px",
                    "padding": "0.75rem 1rem",
                    "background": "linear-gradient(135deg, #e3f2fd 0%, #bbdefb 100%)",
                    "border-radius": "8px",
                    "box-shadow": "0 1px 3px rgba(0,0,0,0.1)",
                    "margin": "0 auto",
                    "max-width": "400px",
                    "border": "1px solid rgba(23, 162, 184, 0.2)"
                })
            ])
        ], width=12)
    ], className="mb-4")
], fluid=True)

# Helper functions (same as before)
def check_file_size(contents, max_size_mb):
    """Check if file size is within the allowed limit"""
    if max_size_mb is None:
        return True, ""  # No size limit
    
    try:
        content_type, content_string = contents.split(',')
        decoded = base64.b64decode(content_string)
        file_size_mb = len(decoded) / (1024 * 1024)  # Convert to MB
        
        if file_size_mb > max_size_mb:
            return False, f"File size ({file_size_mb:.1f} MB) exceeds the maximum allowed size of {max_size_mb} MB"
        
        return True, ""
    except Exception as e:
        return False, f"Error checking file size: {str(e)}"

def save_uploaded_file(contents, filename, session_id):
    """Save uploaded file to session directory"""
    session_dir = os.path.join(TEMP_DIR, session_id)
    os.makedirs(session_dir, exist_ok=True)
    
    content_type, content_string = contents.split(',')
    decoded = base64.b64decode(content_string)
    
    file_path = os.path.join(session_dir, filename)
    with open(file_path, 'wb') as f:
        f.write(decoded)
    
    return file_path

def create_download_zip(session_id, spss_filename=None):
    """Create ZIP file with all results"""
    session_dir = os.path.join(TEMP_DIR, session_id)
    output_dir = os.path.join(session_dir, "outputs")
    
    if not os.path.exists(output_dir):
        return None
    
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    
    # Create filename with SPSS file prefix if available
    if spss_filename:
        # Remove .sav extension and clean filename for use in zip name
        base_name = os.path.splitext(spss_filename)[0]
        # Remove any invalid characters for filename
        import re
        base_name = re.sub(r'[<>:"/\\|?*]', '_', base_name)
        zip_filename = f"{base_name}_Results_{timestamp}.zip"
    else:
        zip_filename = f"SPSS_Results_{timestamp}.zip"
    
    zip_path = os.path.join(session_dir, zip_filename)
    
    with zipfile.ZipFile(zip_path, 'w') as zipf:
        # Add ASCII file
        ascii_file = os.path.join(output_dir, "Data.asc")
        if os.path.exists(ascii_file):
            zipf.write(ascii_file, "Data.asc")
        
        # Add MAP file
        map_file = os.path.join(output_dir, "MAP.xlsx")
        if os.path.exists(map_file):
            zipf.write(map_file, "MAP.xlsx")
        
        # Add log file
        log_file = os.path.join(output_dir, "process_log.log")
        if os.path.exists(log_file):
            zipf.write(log_file, "process_log.log")
    
    return zip_path

# Callbacks

# Toggle Map Upload Visibility
@app.callback(
    Output("map-upload-section", "style"),
    Input("toggle-map", "value")
)
def toggle_map_upload(toggle_value):
    if "use_map" in toggle_value:
        return {"display": "block"}
    return {"display": "none"}

# Handle SPSS File Upload
@app.callback(
    [Output("spss-upload-status", "children"),
     Output("spss-file-data", "data"),
     Output("convert-btn", "disabled")],
    Input("upload-spss", "contents"),
    [State("upload-spss", "filename"),
     State("session-id", "data")]
)
def handle_spss_upload(contents, filename, session_id):
    if contents is None:
        return "", None, True
    
    try:
        if not filename.lower().endswith('.sav'):
            return dbc.Alert("Please upload a valid SPSS (.sav) file", color="danger"), None, True
        
        # Check file size
        size_ok, size_error = check_file_size(contents, CONFIG['MAX_SPSS_FILE_SIZE_MB'])
        if not size_ok:
            return dbc.Alert(size_error, color="danger"), None, True
        
        file_path = save_uploaded_file(contents, filename, session_id)
        
        # Create success message with size info if limits are enabled
        success_msg = f"✓ SPSS file '{filename}' uploaded successfully"
        if CONFIG['MAX_SPSS_FILE_SIZE_MB'] is not None:
            content_type, content_string = contents.split(',')
            decoded = base64.b64decode(content_string)
            file_size_mb = len(decoded) / (1024 * 1024)
            success_msg += f" ({file_size_mb:.1f} MB)"
        
        return dbc.Alert(success_msg, color="success"), {
            "filename": filename,
            "path": file_path
        }, False
        
    except Exception as e:
        return dbc.Alert(f"Error uploading file: {str(e)}", color="danger"), None, True

# Handle Map File Upload
@app.callback(
    [Output("map-upload-status", "children"),
     Output("map-file-data", "data")],
    Input("upload-map", "contents"),
    [State("upload-map", "filename"),
     State("session-id", "data")]
)
def handle_map_upload(contents, filename, session_id):
    if contents is None:
        return "", None
    
    try:
        if not filename.lower().endswith('.xlsx'):
            return dbc.Alert("Please upload a valid Excel (.xlsx) file", color="danger"), None
        
        # Check file size
        size_ok, size_error = check_file_size(contents, CONFIG['MAX_MAP_FILE_SIZE_MB'])
        if not size_ok:
            return dbc.Alert(size_error, color="danger"), None
        
        file_path = save_uploaded_file(contents, filename, session_id)
        
        # Create success message with size info if limits are enabled
        success_msg = f"✓ Map file '{filename}' uploaded successfully"
        if CONFIG['MAX_MAP_FILE_SIZE_MB'] is not None:
            content_type, content_string = contents.split(',')
            decoded = base64.b64decode(content_string)
            file_size_mb = len(decoded) / (1024 * 1024)
            success_msg += f" ({file_size_mb:.1f} MB)"
        
        return dbc.Alert(success_msg, color="success"), {
            "filename": filename,
            "path": file_path
        }
        
    except Exception as e:
        return dbc.Alert(f"Error uploading file: {str(e)}", color="danger"), None

# Main Processing Callback with Enhanced Error Handling
@app.callback(
    [Output("status-alert", "children"),
     Output("status-alert", "is_open"),
     Output("status-alert", "color"),
     Output("progress-section", "style"),
     Output("progress-bar", "value"),
     Output("processing-log", "style"),
     Output("log-content", "children"),
     Output("download-results-btn", "disabled"),
     Output("download-results-btn", "style"),
     Output("progress-interval", "disabled"),
     Output("conversion-counter", "data"),
     Output("counter-display", "children"),
     Output("download-error-map-btn", "disabled"),
     Output("download-error-map-btn", "style"),
     Output("error-map-data", "data")],
    Input("convert-btn", "n_clicks"),
    [State("spss-file-data", "data"),
     State("map-file-data", "data"),
     State("toggle-map", "value"),
     State("zero-padding-toggle", "value"),
     State("width-value-input", "value"),
     State("session-id", "data"),
     State("conversion-counter", "data")]
)
def process_files(n_clicks, spss_data, map_data, use_map, zero_pad, width_val, session_id, current_counter):
    if n_clicks is None or spss_data is None:
        return ("", False, "info", {"display": "none"}, 0, {"display": "none"}, "", 
                True, {"display": "none"}, True, current_counter, 
                f"{current_counter:,}",
                True, {"display": "none"}, None)
    
    try:
        # Setup session directories
        session_dir = os.path.join(TEMP_DIR, session_id)
        output_dir = os.path.join(session_dir, "outputs")
        os.makedirs(output_dir, exist_ok=True)
        
        # Clear previous outputs for clean processing
        if os.path.exists(output_dir):
            shutil.rmtree(output_dir)
            os.makedirs(output_dir, exist_ok=True)
        
        # Setup logging
        logger = setup_logging(output_dir)
        
        # Prepare settings
        settings_config = {
            "zero_padding": "zero_pad" in zero_pad,
            "width_val": width_val or 0
        }
        
        # Process files
        spss_file_path = spss_data["path"]
        
        if "use_map" in use_map and map_data:
            # Process with map file
            map_file_path = map_data["path"]
            process_spss_with_map(spss_file_path, map_file_path, output_dir, settings_config, logger)
        else:
            # Process without map file
            process_spss_wo_map(spss_file_path, output_dir, settings_config, logger)
        
        # Increment counter on successful processing
        new_counter = increment_conversion_counter()
        
        # Read log content with proper file handling
        log_file = os.path.join(output_dir, "process_log.log")
        log_content = ""
        if os.path.exists(log_file):
            try:
                # Add a small delay to ensure file is released
                import time
                time.sleep(0.1)
                with open(log_file, 'r', encoding='utf-8') as f:
                    log_content = f.read()
            except (PermissionError, IOError) as e:
                log_content = f"Could not read log file: {str(e)}"
        
        return (
            "✓ Processing completed successfully! You can now download the results.",
            True,
            "success",
            {"display": "none"},  # Hide progress section after completion
            100,
            {"display": "block"},
            html.Pre(log_content),
            False,
            {"display": "inline-block"},
            True,
            new_counter,
            f"{new_counter:,}",
            True,  # Keep error map button disabled on success
            {"display": "none"},
            None
        )
        
    except Exception as e:
        error_msg = str(e)
        
        # Check if this is a map validation error
        if "Map validation failed" in error_msg and "use_map" in use_map and map_data:
            # Check if error map file exists
            error_map_path = os.path.join(output_dir, "Map_Error.xlsx")
            if os.path.exists(error_map_path):
                # Map validation error - provide download for error map
                return (
                    html.Div([
                        "❌ Map validation failed! Please download the error map to see issues and fix them.",
                        html.Br(),
                        html.Small("Click 'Download Error Map' to see detailed errors.", className="text-muted")
                    ]),
                    True,
                    "danger",
                    {"display": "none"},
                    0,
                    {"display": "block"},
                    html.Pre(f"MAP VALIDATION ERROR:\n{error_msg}\n\nPlease download the error map to see detailed issues."),
                    True,
                    {"display": "none"},
                    True,
                    current_counter,
                    f"{current_counter:,}",
                    False,  # Enable error map download button
                    {"display": "inline-block"},
                    {"path": error_map_path, "filename": "Map_Error.xlsx"}
                )
        
        # General error
        return (
            f"❌ Error during processing: {error_msg}",
            True,
            "danger",
            {"display": "none"},  # Hide progress section on error
            0,
            {"display": "block"},
            html.Pre(f"ERROR: {error_msg}"),
            True,
            {"display": "none"},
            True,
            current_counter,
            f"{current_counter:,}",
            True,  # Keep error map button disabled
            {"display": "none"},
            None
        )

# Show progress section when processing starts
@app.callback(
    [Output("progress-section", "style", allow_duplicate=True),
     Output("status-alert", "is_open", allow_duplicate=True),
     Output("log-content", "children", allow_duplicate=True)],
    Input("convert-btn", "n_clicks"),
    State("spss-file-data", "data"),
    prevent_initial_call=True
)
def show_progress_on_start(n_clicks, spss_data):
    if n_clicks and spss_data:
        # Reset and show progress when starting new conversion
        return {"display": "block"}, False, ""
    return {"display": "none"}, False, ""

# Download button callback
@app.callback(
    Output("download-zip", "data"),
    Input("download-results-btn", "n_clicks"),
    [State("session-id", "data"),
     State("spss-file-data", "data")],
    prevent_initial_call=True
)
def trigger_download(n_clicks, session_id, spss_data):
    if n_clicks:
        try:
            # Get SPSS filename if available
            spss_filename = spss_data.get("filename") if spss_data else None
            
            zip_path = create_download_zip(session_id, spss_filename)
            if zip_path and os.path.exists(zip_path):
                # Read the ZIP file
                with open(zip_path, 'rb') as f:
                    zip_data = f.read()
                
                # Extract filename from the zip path for consistent naming
                filename = os.path.basename(zip_path)
                
                return dcc.send_bytes(zip_data, filename)
            else:
                return None
        except Exception as e:
            print(f"Download error: {str(e)}")
            return None
    return None

# Download template button callback
@app.callback(
    Output("download-template", "data"),
    Input("download-template-btn", "n_clicks"),
    prevent_initial_call=True
)
def download_template(n_clicks):
    if n_clicks:
        try:
            template_path = os.path.join("assets", "External_Map_Template.xlsx")
            if os.path.exists(template_path):
                with open(template_path, 'rb') as f:
                    template_data = f.read()
                return dcc.send_bytes(template_data, "External_Map_Template.xlsx")
            else:
                print(f"Template file not found at: {template_path}")
                return None
        except Exception as e:
            print(f"Template download error: {str(e)}")
            return None
    return None

# New callback for downloading error map
@app.callback(
    Output("download-error-map", "data"),
    Input("download-error-map-btn", "n_clicks"),
    State("error-map-data", "data"),
    prevent_initial_call=True
)
def download_error_map(n_clicks, error_map_data):
    if n_clicks and error_map_data:
        try:
            error_map_path = error_map_data["path"]
            filename = error_map_data["filename"]
            
            if os.path.exists(error_map_path):
                with open(error_map_path, 'rb') as f:
                    file_data = f.read()
                return dcc.send_bytes(file_data, filename)
            else:
                print(f"Error map file not found at: {error_map_path}")
                return None
        except Exception as e:
            print(f"Error map download error: {str(e)}")
            return None
    return None

if __name__ == "__main__":
    app.run(debug=True)