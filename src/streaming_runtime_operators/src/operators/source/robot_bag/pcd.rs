// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Strict PCL PCD v0.7 parser (ascii / binary / binary_compressed).

use anyhow::{Context as _, Result, anyhow, bail};
use super::pcd_body::{
    append_binary_point_fields, decompress_binary_compressed_body, fill_point_object_from_flat_values,
    find_data_body_offset, parse_ascii_rows, PcdDataMode, PcdHeader,
};
use serde_json::{Map, Value, json};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PcdEmitMode {
    Point,
    Cloud,
}

#[derive(Debug, Clone)]
pub struct PcdPointCloud {
    pub header: PcdHeader,
    pub source_path: PathBuf,
    aos_body: Vec<u8>,
    ascii_rows: Vec<Vec<f64>>,
}

impl PcdPointCloud {
    pub fn point_to_json(&self, index: usize) -> Result<Value> {
        if index >= self.header.points {
            bail!("PCD point index {index} out of range (points={})", self.header.points);
        }
        let mut obj = Map::new();
        obj.insert(
            "_source_file".into(),
            Value::String(
                self.source_path
                    .file_name()
                    .and_then(|s| s.to_str())
                    .unwrap_or("")
                    .to_string(),
            ),
        );
        obj.insert("_point_index".into(), json!(index));

        match self.header.data_mode {
            PcdDataMode::Ascii => {
                fill_point_object_from_flat_values(
                    &self.header,
                    &self.ascii_rows[index],
                    &mut obj,
                )?;
            }
            PcdDataMode::Binary | PcdDataMode::BinaryCompressed => {
                append_binary_point_fields(&self.aos_body, &self.header, index, &mut obj)?;
            }
        }
        Ok(Value::Object(obj))
    }

    pub fn cloud_to_json(&self) -> Result<Value> {
        let mut points = Vec::with_capacity(self.header.points);
        for i in 0..self.header.points {
            let mut point = self.point_to_json(i)?;
            if let Some(map) = point.as_object_mut() {
                map.remove("_source_file");
            }
            points.push(point);
        }
        Ok(json!({
            "_source_file": self.source_path.file_name().and_then(|s| s.to_str()).unwrap_or(""),
            "width": self.header.width,
            "height": self.header.height,
            "points": self.header.points,
            "fields": self.header.fields,
            "viewpoint": self.header.viewpoint,
            "data": points,
        }))
    }
}

pub fn load_pcd_paths(path: &Path) -> Result<Vec<PathBuf>> {
    if path.is_file() {
        return Ok(vec![path.to_path_buf()]);
    }
    if path.is_dir() {
        let mut files: Vec<PathBuf> = std::fs::read_dir(path)
            .with_context(|| format!("read pcd dir {}", path.display()))?
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| {
                p.is_file()
                    && p.extension()
                        .is_some_and(|ext| ext.eq_ignore_ascii_case("pcd"))
            })
            .collect();
        files.sort();
        if files.is_empty() {
            bail!("no .pcd files found under '{}'", path.display());
        }
        return Ok(files);
    }
    bail!("pcd path '{}' is neither file nor directory", path.display())
}

pub fn parse_pcd_file(path: &Path) -> Result<PcdPointCloud> {
    let data = std::fs::read(path).with_context(|| format!("read pcd {}", path.display()))?;
    let body_offset = find_data_body_offset(&data)?;
    let header = parse_pcd_header(&data[..body_offset])?;
    let body = &data[body_offset..];

    let (aos_body, ascii_rows) = match header.data_mode {
        PcdDataMode::Ascii => {
            let text = std::str::from_utf8(body).context("pcd ascii body must be utf-8")?;
            (Vec::new(), parse_ascii_rows(text, &header)?)
        }
        PcdDataMode::Binary => {
            let expected = header.points * header.point_step;
            if body.len() < expected {
                bail!(
                    "PCD binary body too short: need {expected} bytes, got {}",
                    body.len()
                );
            }
            (body[..expected].to_vec(), Vec::new())
        }
        PcdDataMode::BinaryCompressed => {
            (decompress_binary_compressed_body(body, &header)?, Vec::new())
        }
    };

    Ok(PcdPointCloud {
        header,
        source_path: path.to_path_buf(),
        aos_body,
        ascii_rows,
    })
}

pub fn pcd_to_messages(
    cloud: &PcdPointCloud,
    emit_mode: PcdEmitMode,
    timestamp_ms: u64,
) -> Result<Vec<(Vec<u8>, u64)>> {
    match emit_mode {
        PcdEmitMode::Point => {
            let mut out = Vec::with_capacity(cloud.header.points);
            for i in 0..cloud.header.points {
                let json = cloud.point_to_json(i)?;
                out.push((serde_json::to_vec(&json)?, timestamp_ms));
            }
            Ok(out)
        }
        PcdEmitMode::Cloud => {
            let json = cloud.cloud_to_json()?;
            Ok(vec![(serde_json::to_vec(&json)?, timestamp_ms)])
        }
    }
}

fn parse_pcd_header(data: &[u8]) -> Result<PcdHeader> {
    let text = std::str::from_utf8(data).context("pcd header must be utf-8")?;
    let mut version = None;
    let mut fields = None;
    let mut sizes = None;
    let mut types = None;
    let mut counts = None;
    let mut width = None;
    let mut height = None;
    let mut viewpoint = None;
    let mut points = None;
    let mut data_mode = None;

    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((key, value)) = line.split_once(' ') else {
            continue;
        };
        let key = key.trim().to_ascii_uppercase();
        let value = value.trim();
        match key.as_str() {
            "VERSION" => version = Some(value.to_string()),
            "FIELDS" => {
                fields = Some(
                    value
                        .split_whitespace()
                        .map(|s| s.to_string())
                        .collect::<Vec<_>>(),
                );
            }
            "SIZE" => sizes = Some(parse_u32_list(value, "SIZE")?),
            "TYPE" => types = Some(parse_type_tokens(value)?),
            "COUNT" => counts = Some(parse_u32_list(value, "COUNT")?),
            "WIDTH" => width = Some(value.parse::<u32>().context("WIDTH")?),
            "HEIGHT" => height = Some(value.parse::<u32>().context("HEIGHT")?),
            "VIEWPOINT" => {
                let vals = parse_f64_list(value, "VIEWPOINT")?;
                if vals.len() != 7 {
                    bail!("VIEWPOINT must contain 7 values, got {}", vals.len());
                }
                let mut vp = [0.0; 7];
                vp.copy_from_slice(&vals);
                viewpoint = Some(vp);
            }
            "POINTS" => points = Some(value.parse::<usize>().context("POINTS")?),
            "DATA" => {
                data_mode = Some(parse_data_mode(value)?);
                break;
            }
            _ => {}
        }
    }

    version.ok_or_else(|| anyhow!("PCD header missing VERSION"))?;
    let fields = fields.ok_or_else(|| anyhow!("PCD header missing FIELDS"))?;
    let sizes = sizes.ok_or_else(|| anyhow!("PCD header missing SIZE"))?;
    let types = types.ok_or_else(|| anyhow!("PCD header missing TYPE"))?;
    let counts = counts.unwrap_or_else(|| vec![1; fields.len()]);
    let width = width.ok_or_else(|| anyhow!("PCD header missing WIDTH"))?;
    let height = height.ok_or_else(|| anyhow!("PCD header missing HEIGHT"))?;
    let viewpoint = viewpoint.unwrap_or([0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0]);
    let points = points.ok_or_else(|| anyhow!("PCD header missing POINTS"))?;
    let data_mode = data_mode.ok_or_else(|| anyhow!("PCD header missing DATA"))?;

    if fields.len() != sizes.len() || fields.len() != types.len() || fields.len() != counts.len()
    {
        bail!(
            "PCD FIELDS/SIZE/TYPE/COUNT length mismatch: {}, {}, {}, {}",
            fields.len(),
            sizes.len(),
            types.len(),
            counts.len()
        );
    }
    for (i, ((&size, &count), &ty)) in sizes.iter().zip(&counts).zip(&types).enumerate() {
        validate_field_type(size, ty)
            .with_context(|| format!("invalid field '{}' at index {}", fields[i], i))?;
        if count == 0 {
            bail!("PCD COUNT for field '{}' must be >= 1", fields[i]);
        }
    }
    if width == 0 || height == 0 {
        bail!("PCD WIDTH/HEIGHT must be > 0");
    }
    if width * height != points as u32 {
        bail!(
            "PCD POINTS ({points}) must equal WIDTH * HEIGHT ({} * {} = {})",
            width,
            height,
            width * height
        );
    }

    let mut field_offsets = Vec::with_capacity(fields.len());
    let mut point_step = 0usize;
    for (&size, &count) in sizes.iter().zip(&counts) {
        field_offsets.push(point_step);
        point_step += (size * count) as usize;
    }

    Ok(PcdHeader {
        fields,
        sizes,
        types,
        counts,
        width,
        height,
        viewpoint,
        points,
        data_mode,
        point_step,
        field_offsets,
    })
}

fn parse_type_tokens(value: &str) -> Result<Vec<char>> {
    let parts: Vec<&str> = value.split_whitespace().collect();
    if parts.is_empty() {
        bail!("PCD TYPE is empty");
    }
    if parts.len() > 1 {
        Ok(parts.iter().filter_map(|p| p.chars().next()).collect())
    } else {
        Ok(parts[0].chars().collect())
    }
}

fn parse_data_mode(value: &str) -> Result<PcdDataMode> {
    match value.to_ascii_lowercase().as_str() {
        "ascii" => Ok(PcdDataMode::Ascii),
        "binary" => Ok(PcdDataMode::Binary),
        "binary_compressed" => Ok(PcdDataMode::BinaryCompressed),
        other => bail!(
            "unsupported PCD DATA mode '{other}'; expected ascii, binary, or binary_compressed"
        ),
    }
}

fn parse_u32_list(value: &str, key: &str) -> Result<Vec<u32>> {
    value
        .split_whitespace()
        .map(|v| v.parse::<u32>().with_context(|| format!("{key} value '{v}'")))
        .collect()
}

fn parse_f64_list(value: &str, key: &str) -> Result<Vec<f64>> {
    value
        .split_whitespace()
        .map(|v| v.parse::<f64>().with_context(|| format!("{key} value '{v}'")))
        .collect()
}

fn validate_field_type(size: u32, ty: char) -> Result<()> {
    match (ty, size) {
        ('I' | 'U', 1 | 2 | 4) | ('F', 4 | 8) => Ok(()),
        (other, sz) => bail!("unsupported PCD TYPE '{other}' with SIZE {sz}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ASCII_PCD: &str = "\
# .PCD v0.7 - Point Cloud Data file format
VERSION .7
FIELDS x y z intensity
SIZE 4 4 4 4
TYPE F F F F
COUNT 1 1 1 1
WIDTH 2
HEIGHT 1
VIEWPOINT 0 0 0 1 0 0 0
POINTS 2
DATA ascii
1.0 2.0 3.0 0.5
4.0 5.0 6.0 0.8
";

    fn write_temp(name: &str, content: impl AsRef<[u8]>) -> PathBuf {
        let dir = std::env::temp_dir().join("fs_pcd_test");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join(name);
        std::fs::write(&path, content.as_ref()).expect("write");
        path
    }

    #[test]
    fn parse_ascii_pcd_header_and_points() {
        let path = write_temp("ascii.pcd", ASCII_PCD);
        let cloud = parse_pcd_file(&path).expect("parse");
        assert_eq!(cloud.header.points, 2);
        assert_eq!(cloud.header.data_mode, PcdDataMode::Ascii);

        let p0 = cloud.point_to_json(0).expect("p0");
        assert_eq!(p0["x"], 1.0);
        assert_eq!(p0["y"], 2.0);
        assert_eq!(p0["z"], 3.0);
        assert_eq!(p0["intensity"], 0.5);
        let p1 = cloud.point_to_json(1).expect("p1");
        assert_eq!(p1["x"], 4.0);

        let msgs = pcd_to_messages(&cloud, PcdEmitMode::Point, 0).expect("msgs");
        assert_eq!(msgs.len(), 2);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn parse_binary_pcd_xyz() {
        let mut body: Vec<u8> = Vec::new();
        for (x, y, z) in [(1.0_f32, 2.0_f32, 3.0_f32), (4.0_f32, 5.0_f32, 6.0_f32)] {
            body.extend_from_slice(&x.to_le_bytes());
            body.extend_from_slice(&y.to_le_bytes());
            body.extend_from_slice(&z.to_le_bytes());
        }
        let header = r"# .PCD v0.7
VERSION .7
FIELDS x y z
SIZE 4 4 4
TYPE F F F
COUNT 1 1 1
WIDTH 2
HEIGHT 1
VIEWPOINT 0 0 0 1 0 0 0
POINTS 2
DATA binary
";
        let mut file = header.as_bytes().to_vec();
        file.extend(body);
        let path = write_temp("binary.pcd", file);
        let cloud = parse_pcd_file(&path).expect("parse");
        let p0 = cloud.point_to_json(0).expect("p0");
        assert!((p0["x"].as_f64().unwrap() - 1.0).abs() < 1e-5);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn parse_binary_compressed_pcd() {
        let points = 64usize;
        let mut aos = Vec::with_capacity(points * 12);
        for i in 0..points {
            let f = i as f32;
            aos.extend_from_slice(&f.to_le_bytes());
            aos.extend_from_slice(&(f + 100.0).to_le_bytes());
            aos.extend_from_slice(&(f + 200.0).to_le_bytes());
        }

        let mut soa = vec![0u8; points * 12];
        for field in 0..3usize {
            let field_bytes = 4;
            let soa_base = field * points * field_bytes;
            for pt in 0..points {
                let aos_off = pt * 12 + field * field_bytes;
                let dst = soa_base + pt * field_bytes;
                soa[dst..dst + field_bytes].copy_from_slice(&aos[aos_off..aos_off + field_bytes]);
            }
        }

        let compressed = lzf::compress(&soa).expect("compress");
        let mut body = Vec::new();
        body.extend_from_slice(&(compressed.len() as u32).to_le_bytes());
        body.extend_from_slice(&(soa.len() as u32).to_le_bytes());
        body.extend_from_slice(&compressed);

        let header = format!(
            "# .PCD v0.7\n\
VERSION .7\n\
FIELDS x y z\n\
SIZE 4 4 4\n\
TYPE F F F\n\
COUNT 1 1 1\n\
WIDTH {points}\n\
HEIGHT 1\n\
VIEWPOINT 0 0 0 1 0 0 0\n\
POINTS {points}\n\
DATA binary_compressed\n"
        );
        let mut file = header.into_bytes();
        file.extend(body);
        let path = write_temp("compressed.pcd", file);
        let cloud = parse_pcd_file(&path).expect("parse");
        let p1 = cloud.point_to_json(1).expect("p1");
        assert!((p1["x"].as_f64().unwrap() - 1.0).abs() < 1e-5);
        assert!((p1["z"].as_f64().unwrap() - 201.0).abs() < 1e-5);

        let cloud_msgs = pcd_to_messages(&cloud, PcdEmitMode::Cloud, 0).expect("cloud");
        assert_eq!(cloud_msgs.len(), 1);
        let _ = std::fs::remove_file(path);
    }
}
