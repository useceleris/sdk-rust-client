use crate::constants::{
    MAXIMUM_COMMAND_NAME_BYTES, MAXIMUM_DEPTH, MAXIMUM_ERROR_NAME_BYTES, MAXIMUM_FRAGMENTS,
    MAXIMUM_INTEGER32_LINE_BYTES, MAXIMUM_INTEGER64_LINE_BYTES,
};
use crate::errors::{
    CelerisError, ServerError, ServerErrorResource, ServerErrorType, protocol_error,
};
use crate::messages::{Delivery, PresenceListResponse, ServerMessage};
use crate::values::{PresenceConnection, PresenceEvent, PresencePage, ServerNotice};

type Decoded<Value> = Result<Value, CelerisError>;

/// Decodes one transport message. A decoder is built per message over that
/// message's own bytes, so nothing spans messages and a malformed one cannot
/// desynchronise the next (DECODE-01).
pub(crate) fn decode_server_message(data: &[u8]) -> Decoded<ServerMessage> {
    let mut decoder = MessageDecoder {
        data,
        offset: 0,
        fragments: 0,
    };

    let message = decoder.read_message(0, true)?;

    if decoder.offset != data.len() {
        return Err(protocol_error(
            "Trailing data after server message.",
            "Message",
            decoder.offset,
        ));
    }

    Ok(message)
} // end fn decode_server_message

struct MessageDecoder<'data> {
    data: &'data [u8],
    offset: usize,
    fragments: usize,
} // end struct MessageDecoder

impl<'data> MessageDecoder<'data> {
    fn read_marker(&mut self, field: &'static str) -> Decoded<u8> {
        let field_start_offset = self.offset;
        self.fragments += 1;

        if self.fragments > MAXIMUM_FRAGMENTS {
            return Err(protocol_error(
                "Server message has more than 4096 fragments.",
                field,
                field_start_offset,
            ));
        }

        let Some(&marker) = self.data.get(self.offset) else {
            return Err(protocol_error(
                "Missing field marker.",
                field,
                field_start_offset,
            ));
        };

        self.offset += 1;

        Ok(marker)
    } // end method read_marker

    /// Reads up to LF, allowing one CR before it, and returns the content
    /// between. The scan never runs past the content limit, one CR and the LF.
    fn read_line(
        &mut self,
        field: &'static str,
        field_start_offset: usize,
        maximum_length: usize,
    ) -> Decoded<&'data [u8]> {
        let data = self.data;
        let line_start = self.offset;
        let search_end = data
            .len()
            .min(line_start.saturating_add(maximum_length).saturating_add(2));
        let too_long = format!("Line exceeds its {maximum_length}-byte limit.");

        let Some(newline) = data[line_start..search_end]
            .iter()
            .position(|&byte| byte == b'\n')
        else {
            if search_end - line_start == maximum_length.saturating_add(2) {
                return Err(protocol_error(&too_long, field, field_start_offset));
            }

            return Err(protocol_error(
                "Unterminated line.",
                field,
                field_start_offset,
            ));
        };

        let mut content_end = line_start + newline;
        self.offset = content_end + 1;

        if content_end > line_start && data[content_end - 1] == b'\r' {
            content_end -= 1;
        }

        if content_end - line_start > maximum_length {
            return Err(protocol_error(&too_long, field, field_start_offset));
        }

        Ok(&data[line_start..content_end])
    } // end method read_line

    /// Reads a simple string, bounded only by the message.
    fn read_unbounded_line(
        &mut self,
        field: &'static str,
        field_start_offset: usize,
    ) -> Decoded<&'data [u8]> {
        self.read_line(field, field_start_offset, self.data.len())
    } // end method read_unbounded_line

    /// Reads the decimal grammar that every numeric field shares: an optional
    /// minus sign, then digits only, within the given range.
    fn read_decimal(
        &mut self,
        field: &'static str,
        field_start_offset: usize,
        maximum_line_bytes: usize,
        minimum: i64,
        maximum: i64,
    ) -> Decoded<i64> {
        let line = self.read_line(field, field_start_offset, maximum_line_bytes)?;
        let digits = line.strip_prefix(b"-").unwrap_or(line);

        if digits.is_empty() || !digits.iter().all(u8::is_ascii_digit) {
            return Err(protocol_error(
                "Expected decimal digits.",
                field,
                field_start_offset,
            ));
        }

        let value = std::str::from_utf8(line)
            .ok()
            .and_then(|text| text.parse::<i64>().ok())
            .filter(|value| (minimum..=maximum).contains(value));

        value.ok_or_else(|| {
            protocol_error(
                &format!("Integer is outside {minimum} to {maximum}."),
                field,
                field_start_offset,
            )
        })
    } // end method read_decimal

    fn read_integer64_digits(
        &mut self,
        field: &'static str,
        field_start_offset: usize,
    ) -> Decoded<i64> {
        self.read_decimal(
            field,
            field_start_offset,
            MAXIMUM_INTEGER64_LINE_BYTES,
            i64::MIN,
            i64::MAX,
        )
    } // end method read_integer64_digits

    fn read_integer32_digits(
        &mut self,
        field: &'static str,
        field_start_offset: usize,
    ) -> Decoded<i32> {
        let value = self.read_decimal(
            field,
            field_start_offset,
            MAXIMUM_INTEGER32_LINE_BYTES,
            i64::from(i32::MIN),
            i64::from(i32::MAX),
        )?;

        // The range check above makes the conversion exact.
        Ok(value as i32)
    } // end method read_integer32_digits

    /// Reads an Integer64, which carries timestamps only.
    fn read_timestamp(&mut self) -> Decoded<i64> {
        let field_start_offset = self.offset;

        if self.read_marker("Timestamp")? != b':' {
            return Err(protocol_error(
                "Expected Integer64 marker.",
                "Timestamp",
                field_start_offset,
            ));
        }

        self.read_integer64_digits("Timestamp", field_start_offset)
    } // end method read_timestamp

    fn read_integer32(&mut self, field: &'static str) -> Decoded<i32> {
        let field_start_offset = self.offset;

        if self.read_marker(field)? != b';' {
            return Err(protocol_error(
                "Expected Integer32 marker.",
                field,
                field_start_offset,
            ));
        }

        self.read_integer32_digits(field, field_start_offset)
    } // end method read_integer32

    /// Reads a simple or bulk string; null is `None`.
    fn read_bytes(&mut self, field: &'static str) -> Decoded<Option<&'data [u8]>> {
        let field_start_offset = self.offset;

        match self.read_marker(field)? {
            b'+' => self
                .read_unbounded_line(field, field_start_offset)
                .map(Some),
            b'$' => self.read_bulk_bytes(field, field_start_offset),
            _ => Err(protocol_error(
                "Expected simple or bulk byte marker.",
                field,
                field_start_offset,
            )),
        }
    } // end method read_bytes

    fn read_bulk_bytes(
        &mut self,
        field: &'static str,
        field_start_offset: usize,
    ) -> Decoded<Option<&'data [u8]>> {
        let data = self.data;
        let length = self.read_integer64_digits(field, field_start_offset)?;

        if length == -1 {
            return Ok(None);
        }

        let Ok(length) = usize::try_from(length) else {
            return Err(protocol_error(
                "Invalid bulk byte length.",
                field,
                field_start_offset,
            ));
        };

        if length > data.len() - self.offset {
            return Err(protocol_error(
                "Bulk payload exceeds remaining message bytes.",
                field,
                field_start_offset,
            ));
        }

        let value = &data[self.offset..self.offset + length];
        self.offset += length;

        if data.get(self.offset) == Some(&b'\r') {
            self.offset += 1;
        }

        if data.get(self.offset) != Some(&b'\n') {
            return Err(protocol_error(
                "Missing bulk byte terminator.",
                field,
                field_start_offset,
            ));
        }

        self.offset += 1;

        Ok(Some(value))
    } // end method read_bulk_bytes

    fn read_identifier(&mut self, field: &'static str) -> Decoded<String> {
        let field_start_offset = self.offset;

        self.read_nullable_identifier(field)?
            .ok_or_else(|| protocol_error("Identifier cannot be null.", field, field_start_offset))
    } // end method read_identifier

    fn read_nullable_identifier(&mut self, field: &'static str) -> Decoded<Option<String>> {
        let field_start_offset = self.offset;

        let Some(value) = self.read_bytes(field)? else {
            return Ok(None);
        };

        let identifier = read_text(value, field, field_start_offset)?;

        if identifier.is_empty() || identifier.contains(['\r', '\n']) {
            return Err(protocol_error(
                "Identifier must be nonempty and CR/LF-free.",
                field,
                field_start_offset,
            ));
        }

        Ok(Some(identifier))
    } // end method read_nullable_identifier

    /// Copies the payload once, so what the caller receives does not share
    /// memory with the transport message.
    fn read_payload(&mut self, field: &'static str) -> Decoded<Vec<u8>> {
        let field_start_offset = self.offset;

        match self.read_bytes(field)? {
            Some(value) => Ok(value.to_vec()),
            None => Err(protocol_error(
                "Payload cannot be null.",
                field,
                field_start_offset,
            )),
        }
    } // end method read_payload

    fn read_array_length(
        &mut self,
        depth: usize,
        field: &'static str,
        marker_already_read: bool,
    ) -> Decoded<usize> {
        let field_start_offset = if marker_already_read {
            self.offset - 1
        } else {
            self.offset
        };

        if depth >= MAXIMUM_DEPTH {
            return Err(protocol_error(
                "Arrays are nested deeper than 32 levels.",
                field,
                field_start_offset,
            ));
        }

        if !marker_already_read && self.read_marker(field)? != b'*' {
            return Err(protocol_error(
                "Expected array marker.",
                field,
                field_start_offset,
            ));
        }

        let length = self.read_integer64_digits(field, field_start_offset)?;

        let Ok(length) = usize::try_from(length) else {
            return Err(protocol_error(
                "Array length cannot be negative.",
                field,
                field_start_offset,
            ));
        };

        if length > MAXIMUM_FRAGMENTS - self.fragments {
            return Err(protocol_error(
                "Array length exceeds the 4096-fragment budget.",
                field,
                field_start_offset,
            ));
        }

        Ok(length)
    } // end method read_array_length

    fn read_connections(&mut self, depth: usize) -> Decoded<Vec<PresenceConnection>> {
        let length = self.read_array_length(depth, "Connections", false)?;
        let mut connections = Vec::with_capacity(length);

        for _ in 0..length {
            let field_start_offset = self.offset;

            if self.read_array_length(depth + 1, "Connection", false)? != 3 {
                return Err(protocol_error(
                    "Presence connection must contain three fields.",
                    "Connection",
                    field_start_offset,
                ));
            }

            connections.push(PresenceConnection {
                token_reference: self.read_identifier("TokenReference")?,
                connection_id: self.read_identifier("ConnectionID")?,
                timestamp: self.read_timestamp()?,
            });
        }

        Ok(connections)
    } // end method read_connections

    fn read_message(&mut self, depth: usize, tail: bool) -> Decoded<ServerMessage> {
        let field_start_offset = self.offset;

        match self.read_marker("Message")? {
            b'*' => self.read_message_array(depth, tail),
            b'-' => self.read_error_message(depth),
            b'@' => self.read_command_message(depth, tail),
            _ => Err(protocol_error(
                "Unexpected server message marker.",
                "Message",
                field_start_offset,
            )),
        }
    } // end method read_message

    fn read_message_array(&mut self, depth: usize, tail: bool) -> Decoded<ServerMessage> {
        let length = self.read_array_length(depth, "Messages", true)?;
        let mut messages = Vec::with_capacity(length);

        for index in 0..length {
            messages.push(self.read_message(depth + 1, tail && index == length - 1)?);
        }

        Ok(ServerMessage::Array(messages))
    } // end method read_message_array

    /// Reads an error frame. Every field is self-delimiting, so an error may
    /// sit anywhere in a batch.
    fn read_error_message(&mut self, depth: usize) -> Decoded<ServerMessage> {
        let field_start_offset = self.offset - 1;

        if self.read_line("Error", field_start_offset, 3)? != b"Err" {
            return Err(protocol_error(
                "Invalid error header.",
                "Error",
                field_start_offset,
            ));
        }

        let error_type = self.read_error_type()?;
        let sub_type = self.read_error_sub_type()?;
        let message = self.read_payload("ErrorMessage")?;
        let resource = self.read_resource(depth)?;

        // The server's message is read without failing on malformed bytes, so
        // a bad message can never break error delivery (ERR-01).
        Ok(ServerMessage::Error(ServerError {
            error_type: ServerErrorType::from_wire(error_type),
            sub_type,
            message: String::from_utf8_lossy(&message).into_owned(),
            resource,
        }))
    } // end method read_error_message

    fn read_error_type(&mut self) -> Decoded<String> {
        let field_start_offset = self.offset;

        if self.read_marker("ErrorType")? != b'+' {
            return Err(protocol_error(
                "Expected simple string marker.",
                "ErrorType",
                field_start_offset,
            ));
        }

        self.read_error_name("ErrorType", field_start_offset)
    } // end method read_error_type

    fn read_error_sub_type(&mut self) -> Decoded<Option<String>> {
        let field_start_offset = self.offset;

        match self.read_marker("ErrorSubType")? {
            b'+' => {
                return self
                    .read_error_name("ErrorSubType", field_start_offset)
                    .map(Some);
            }
            b'$' if self.read_integer64_digits("ErrorSubType", field_start_offset)? == -1 => {
                return Ok(None);
            }
            _ => {}
        }

        Err(protocol_error(
            "Sub type must be a simple string or null.",
            "ErrorSubType",
            field_start_offset,
        ))
    } // end method read_error_sub_type

    /// Reads an error type or sub type: a bounded name of letters, digits and
    /// underscores that starts with a letter, such as `PermissionDeniedError`
    /// or `PRES_LIST`.
    fn read_error_name(
        &mut self,
        field: &'static str,
        field_start_offset: usize,
    ) -> Decoded<String> {
        let name = self.read_line(field, field_start_offset, MAXIMUM_ERROR_NAME_BYTES)?;

        if !valid_error_name(name) {
            return Err(protocol_error(
                "Invalid error name.",
                field,
                field_start_offset,
            ));
        }

        // A valid name is ASCII.
        Ok(String::from_utf8_lossy(name).into_owned())
    } // end method read_error_name

    /// Reads any single fragment that an error's type and sub type define:
    /// null, a string, an Integer64, an Integer32, or an array of these.
    fn read_resource(&mut self, depth: usize) -> Decoded<ServerErrorResource> {
        let field_start_offset = self.offset;

        match self.read_marker("Resource")? {
            b'+' => {
                let line = self.read_unbounded_line("Resource", field_start_offset)?;

                read_text(line, "Resource", field_start_offset).map(ServerErrorResource::Text)
            }
            b'$' => match self.read_bulk_bytes("Resource", field_start_offset)? {
                Some(value) => {
                    read_text(value, "Resource", field_start_offset).map(ServerErrorResource::Text)
                }
                None => Ok(ServerErrorResource::Null),
            },
            b':' => self
                .read_integer64_digits("Resource", field_start_offset)
                .map(ServerErrorResource::Integer64),
            b';' => self
                .read_integer32_digits("Resource", field_start_offset)
                .map(ServerErrorResource::Integer32),
            b'*' => {
                let length = self.read_array_length(depth + 1, "Resource", true)?;
                let mut items = Vec::with_capacity(length);

                for _ in 0..length {
                    items.push(self.read_resource(depth + 1)?);
                }

                Ok(ServerErrorResource::Array(items))
            }
            _ => Err(protocol_error(
                "Unexpected resource marker.",
                "Resource",
                field_start_offset,
            )),
        }
    } // end method read_resource

    fn read_command_message(&mut self, depth: usize, tail: bool) -> Decoded<ServerMessage> {
        let field_start_offset = self.offset - 1;
        let line = self.read_line("Command", field_start_offset, MAXIMUM_COMMAND_NAME_BYTES)?;

        match read_text(line, "Command", field_start_offset)?.as_str() {
            "MSG" => self.read_delivery(),
            "SERVER_MSG" => self.read_notice(),
            "PRES_NOTIFY" => self.read_presence_notification(),
            "PRES_LIST_RESPONSE" => self.read_presence_response(depth),
            _ => self.skip_unknown_command(depth, tail, field_start_offset),
        }
    } // end method read_command_message

    /// Skips a command this version does not know. It carries an unknown
    /// number of fields, so its end is knowable only when it runs to the end of
    /// the transport message. Newer servers may add commands; skipping them
    /// keeps this client working instead of dropping its connection
    /// (DECODE-01).
    fn skip_unknown_command(
        &mut self,
        depth: usize,
        tail: bool,
        field_start_offset: usize,
    ) -> Decoded<ServerMessage> {
        if depth != 0 && !tail {
            return Err(protocol_error(
                "Unknown command inside array has ambiguous boundaries.",
                "Command",
                field_start_offset,
            ));
        }

        self.offset = self.data.len();

        Ok(ServerMessage::Ignored)
    } // end method skip_unknown_command

    fn read_delivery(&mut self) -> Decoded<ServerMessage> {
        Ok(ServerMessage::Delivery(Delivery {
            token_reference: self.read_identifier("TokenReference")?,
            segment_id: self.read_identifier("SegmentID")?,
            message_id: self.read_nullable_identifier("MessageID")?,
            timestamp: self.read_timestamp()?,
            payload: self.read_payload("Payload")?,
        }))
    } // end method read_delivery

    fn read_notice(&mut self) -> Decoded<ServerMessage> {
        Ok(ServerMessage::Notice(ServerNotice {
            timestamp: self.read_timestamp()?,
            payload: self.read_payload("Payload")?,
        }))
    } // end method read_notice

    fn read_presence_notification(&mut self) -> Decoded<ServerMessage> {
        let segment_id = self.read_identifier("SegmentID")?;
        let token_reference = self.read_identifier("TokenReference")?;
        let connection_id = self.read_identifier("ConnectionID")?;
        let event_offset = self.offset;

        // A join/leave flag, not metadata: narrowed here rather than passed
        // through raw, and any other value is not a flag this client knows.
        let joined = match self.read_integer32("Event")? {
            1 => true,
            0 => false,
            _ => {
                return Err(protocol_error(
                    "Presence event must be 0 or 1.",
                    "Event",
                    event_offset,
                ));
            }
        };

        Ok(ServerMessage::PresenceNotify(PresenceEvent {
            segment_id,
            token_reference,
            connection_id,
            joined,
            timestamp: self.read_timestamp()?,
        }))
    } // end method read_presence_notification

    fn read_presence_response(&mut self, depth: usize) -> Decoded<ServerMessage> {
        let segment_id = self.read_identifier("SegmentID")?;
        let request_id = self.read_identifier("RequestID")?;

        let page = PresencePage {
            segment_id,
            total: self.read_integer32("Total")?,
            per_page: self.read_integer32("PerPage")?,
            current_page: self.read_integer32("CurrentPage")?,
            from: self.read_integer32("From")?,
            to: self.read_integer32("To")?,
            connections: self.read_connections(depth)?,
        };

        Ok(ServerMessage::PresenceList(PresenceListResponse {
            request_id,
            page,
        }))
    } // end method read_presence_response
} // end impl MessageDecoder

fn read_text(data: &[u8], field: &'static str, field_start_offset: usize) -> Decoded<String> {
    match std::str::from_utf8(data) {
        Ok(text) => Ok(text.to_owned()),
        Err(_) => Err(protocol_error(
            "Invalid UTF-8 text.",
            field,
            field_start_offset,
        )),
    }
} // end fn read_text

fn valid_error_name(name: &[u8]) -> bool {
    let Some((&first, rest)) = name.split_first() else {
        return false;
    };

    first.is_ascii_alphabetic()
        && rest
            .iter()
            .all(|&byte| byte.is_ascii_alphanumeric() || byte == b'_')
} // end fn valid_error_name
