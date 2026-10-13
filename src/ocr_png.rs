//! Sequential tiles for very tall PNGs: never allocate their complete raster.
use anyhow::{anyhow, Context, Result};
use image::RgbImage;
use std::{
    fs::File,
    io::{BufReader, Read, Seek, SeekFrom},
    path::Path,
    time::Instant,
};

const ROWS: usize = 512;
const ROW_OVERLAP: usize = 128;
const COLUMNS: usize = 1024;
const COLUMN_OVERLAP: usize = 256;
const WHOLE_FRAME_THRESHOLD: u64 = 128 * 1024 * 1024;

/// None keeps the existing decoder for ordinary images and non-PNG content.
pub(crate) fn stream_tiles(
    path: &Path,
    deadline: Instant,
    mut infer: impl FnMut(RgbImage, u32, u32) -> Result<()>,
) -> Result<Option<usize>> {
    let mut file = File::open(path)?;
    let mut signature = [0u8; 8];
    if file.read_exact(&mut signature).is_err() || signature != *b"\x89PNG\r\n\x1a\n" {
        return Ok(None);
    }
    file.seek(SeekFrom::Start(0))?;
    let mut decoder = png::Decoder::new(BufReader::new(file));
    decoder.set_limits(png::Limits {
        bytes: 64 * 1024 * 1024,
    });
    decoder.set_transformations(png::Transformations::EXPAND | png::Transformations::STRIP_16);
    let mut reader = decoder.read_info().context("read streaming PNG header")?;
    let (width, height, interlaced) = (
        reader.info().width as usize,
        reader.info().height as usize,
        reader.info().interlaced,
    );
    if width as u64 * height as u64 * 4 <= WHOLE_FRAME_THRESHOLD {
        return Ok(None);
    }
    if interlaced
        || width == 0
        || width > 8192
        || height == 0
        || height > 1_000_000
        || width as u64 * height as u64 * 4 > 32 * 1024 * 1024 * 1024
    {
        return Err(anyhow!("PNG exceeds bounded sequential tile geometry"));
    }
    let (color, depth) = reader.output_color_type();
    if depth != png::BitDepth::Eight
        || !matches!(
            color,
            png::ColorType::Rgb
                | png::ColorType::Rgba
                | png::ColorType::Grayscale
                | png::ColorType::GrayscaleAlpha
        )
    {
        return Err(anyhow!("PNG streaming output format is unsupported"));
    }
    let channels = color.samples();
    let stride = width * 3;
    let mut strip = Vec::with_capacity(stride * ROWS);
    let mut read_rows = 0usize;
    let mut offset = 0usize;
    let mut emitted = 0usize;
    while let Some(row) = reader.next_row().context("decode streaming PNG row")? {
        if Instant::now() >= deadline {
            return Err(anyhow!("PNG tile OCR deadline exceeded"));
        }
        if row.data().len() != width * channels || read_rows >= height {
            return Err(anyhow!("PNG streaming row geometry changed"));
        }
        for pixel in row.data().chunks_exact(channels) {
            strip.extend_from_slice(&rgb_pixel(pixel));
        }
        read_rows += 1;
        if strip.len() == stride * ROWS {
            emitted += emit_strip(&strip, width, offset, deadline, &mut infer)?;
            strip.drain(..stride * (ROWS - ROW_OVERLAP));
            offset += ROWS - ROW_OVERLAP;
        }
    }
    if read_rows != height {
        return Err(anyhow!("PNG streaming frame incomplete"));
    }
    // Check all remaining chunk CRCs/IEND; no partial output survives failure.
    reader.finish().context("finish streaming PNG frame")?;
    if Instant::now() >= deadline {
        return Err(anyhow!("PNG tile OCR deadline exceeded"));
    }
    if !strip.is_empty() {
        emitted += emit_strip(&strip, width, offset, deadline, &mut infer)?;
    }
    if Instant::now() >= deadline {
        return Err(anyhow!("PNG tile OCR deadline exceeded"));
    }
    Ok(Some(emitted))
}

fn rgb_pixel(pixel: &[u8]) -> [u8; 3] {
    let rgb = if pixel.len() < 3 {
        [pixel[0]; 3]
    } else {
        [pixel[0], pixel[1], pixel[2]]
    };
    if matches!(pixel.len(), 2 | 4) {
        let alpha = *pixel.last().unwrap() as u32;
        // Transparent backgrounds are rendered on white before OCR/blank tests.
        rgb.map(|value| ((value as u32 * alpha + 255 * (255 - alpha) + 127) / 255) as u8)
    } else {
        rgb
    }
}

fn emit_strip(
    strip: &[u8],
    width: usize,
    y: usize,
    deadline: Instant,
    infer: &mut impl FnMut(RgbImage, u32, u32) -> Result<()>,
) -> Result<usize> {
    let height = strip.len() / (width * 3);
    let mut emitted = 0;
    let mut x = 0;
    while x < width {
        if Instant::now() >= deadline {
            return Err(anyhow!("PNG tile OCR deadline exceeded"));
        }
        let columns = COLUMNS.min(width - x);
        let mut tile = Vec::with_capacity(columns * height * 3);
        for row in strip.chunks_exact(width * 3) {
            tile.extend_from_slice(&row[x * 3..(x + columns) * 3]);
        }
        let first = &tile[..3];
        // Exact constant color has no visible text. No heuristic blank filter.
        if tile.chunks_exact(3).any(|pixel| pixel != first) {
            infer(
                RgbImage::from_raw(columns as u32, height as u32, tile)
                    .ok_or_else(|| anyhow!("PNG tile shape invalid"))?,
                x as u32,
                y as u32,
            )?;
            if Instant::now() >= deadline {
                return Err(anyhow!("PNG tile OCR deadline exceeded"));
            }
            emitted += 1;
        }
        if x + columns == width {
            break;
        }
        x += COLUMNS - COLUMN_OVERLAP;
    }
    Ok(emitted)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    #[test]
    fn alpha_only_text_keeps_visible_contrast_before_uniform_tile_filter() {
        assert_eq!(rgb_pixel(&[0, 0, 0, 0]), [255; 3]);
        assert_eq!(rgb_pixel(&[0, 0, 0, 255]), [0; 3]);
        assert_eq!(rgb_pixel(&[0, 0]), [255; 3]);
        assert_eq!(rgb_pixel(&[0, 255]), [0; 3]);
    }
    #[test]
    fn last_inference_crossing_deadline_is_not_success() {
        let strip = [0, 0, 0, 255, 255, 255];
        assert!(emit_strip(
            &strip,
            2,
            0,
            Instant::now() + Duration::from_millis(10),
            &mut |_, _, _| {
                std::thread::sleep(Duration::from_millis(20));
                Ok(())
            }
        )
        .is_err());
    }
    #[test]
    fn row_tiles_cover_edges_with_overlap_without_whole_frame_allocation() {
        let width = 2301;
        let height = 510;
        let mut strip = vec![255; width * height * 3];
        for (x, y) in [(0, 0), (1023, 509), (2300, 509)] {
            strip[(y * width + x) * 3] = 0;
        }
        let mut seen = Vec::new();
        let count = emit_strip(
            &strip,
            width,
            1234,
            Instant::now() + Duration::from_secs(2),
            &mut |tile, x, y| {
                assert!(tile.width() <= 1024 && tile.height() <= 512);
                assert_eq!(y, 1234);
                seen.push((x, tile.width()));
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(count, 3);
        assert_eq!(seen, vec![(0, 1024), (768, 1024), (1536, 765)]);
    }
    #[test]
    fn uniform_tiles_skip_inference_but_deadline_never_becomes_empty_success() {
        let strip = vec![255; 100 * 512 * 3];
        let mut calls = 0;
        assert_eq!(
            emit_strip(
                &strip,
                100,
                0,
                Instant::now() + Duration::from_secs(1),
                &mut |_, _, _| {
                    calls += 1;
                    Ok(())
                }
            )
            .unwrap(),
            0
        );
        assert_eq!(calls, 0);
        assert!(emit_strip(&strip, 100, 0, Instant::now(), &mut |_, _, _| Ok(())).is_err());
    }
    #[test]
    fn real_tall_png_decodes_all_rows_and_bottom_tile_in_a_bounded_strip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tall.png");
        let mut encoder = png::Encoder::new(File::create(&path).unwrap(), 100, 340_000);
        encoder.set_color(png::ColorType::Rgb);
        encoder.set_depth(png::BitDepth::Eight);
        let writer = encoder.write_header().unwrap();
        let mut stream = writer.into_stream_writer().unwrap();
        use std::io::Write;
        let row = [255u8; 300];
        for y in 0..340_000 {
            let mut pixels = row;
            if y == 339_999 {
                pixels[299] = 0;
            }
            stream.write_all(&pixels).unwrap();
        }
        stream.finish().unwrap();
        let mut bottom = false;
        let count = stream_tiles(
            &path,
            Instant::now() + Duration::from_secs(10),
            |tile, _, y| {
                bottom |= y + tile.height() == 340_000;
                Ok(())
            },
        )
        .unwrap()
        .unwrap();
        assert!(bottom);
        assert!(count > 0 && count <= 2);
    }
}
