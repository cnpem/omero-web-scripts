#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Expand metadata annotations (Tags, Key-Values, Files, Comments) from containers
(Project, Dataset, Screen, Plate, Well) to their Images.

Key features:
- Batch-fetch existing annotations for all targets at once
- Batch-save annotation links in chunks (1000 per transaction)
- Supports HCS (Screen/Plate/Well) and general (Project/Dataset) hierarchies
- Cascade support for Screen level
- Prevents duplicate annotation links
"""

from typing import Generator, Optional

from omero import scripts
from omero.gateway import BlitzGateway
from omero.model import (
    ImageAnnotationLinkI,
    WellAnnotationLinkI,
    PlateAnnotationLinkI,
    ImageI,
    PlateI,
    WellI
)
from omero.rtypes import rlong, rstring
from omero.sys import Filter, ParametersI

# =============================================================================
# Constants
# =============================================================================

ANNOTATION_TYPES = {
    "Tag": "TagAnnotationI",
    "Key-Value": "MapAnnotationI",
    "File": "FileAnnotationI",
    "Comment": "CommentAnnotationI",
    "All": None,
}

SUPPORTED_TYPES = frozenset({"Project", "Dataset", "Screen", "Plate", "Well"})
HCS_TYPES = frozenset({"Screen", "Plate", "Well"})
GENERAL_TYPES = frozenset({"Project", "Dataset"})
BATCH_SIZE = 1000 

# =============================================================================
# Logging & Utilities
# =============================================================================

def log(message: str) -> None:
    print(message)

def get_annotation_key(link) -> str:
    """Create a unique key for an annotation link."""
    try:
        ann_id = link.child.id.val
        parent_id = link.parent.id.val
    except AttributeError:
        ann_id = link.child.id
        parent_id = link.parent.id
    return f"{parent_id}_{ann_id}"

def create_params(conn: BlitzGateway) -> ParametersI:
    params = ParametersI()
    if not conn.isAdmin():
        params.theFilter = Filter()
        params.theFilter.ownerId = rlong(conn.getUserId())
    return params

# =============================================================================
# Hierarchy Navigation
# =============================================================================

def get_images_from_object(obj, dtype: str):
    """Yield all images under an OMERO container."""
    if dtype == "Well":
        for ws in obj.listChildren():
            if ws.getImage():
                yield ws.getImage()
    elif dtype == "Plate":
        for well in obj.listChildren():
            for ws in well.listChildren():
                if ws.getImage():
                    yield ws.getImage()
    elif dtype == "Screen":
        for plate in obj.listChildren():
            for well in plate.listChildren():
                for ws in well.listChildren():
                    if ws.getImage():
                        yield ws.getImage()
    elif dtype == "Dataset":
        yield from obj.listChildren()
    elif dtype == "Project":
        for ds in obj.listChildren():
            yield from ds.listChildren()

def get_source_from_hierarchy(obj, dtype: str, source_level: str):
    """Navigate hierarchy to find the source object for annotations."""
    if dtype == source_level:
        return obj
    
    # HCS hierarchy navigation
    if dtype == "Well":
        if source_level == "Plate": 
            return obj.getParent()
        if source_level == "Screen":
            plate = obj.getParent()
            return plate.getParent() if plate else None
    if dtype == "Plate" and source_level == "Screen":
        return obj.getParent()
    
    # General hierarchy navigation
    if dtype == "Dataset" and source_level == "Project":
        return obj.getParent()

    return None

# =============================================================================
# Annotation Copy Logic
# =============================================================================

def copy_annotations_to_targets(
    conn: BlitzGateway,
    source_obj,
    target_objects: list,
    target_type: str,
    ann_filter: Optional[str],
) -> int:
    if not source_obj or not target_objects:
        return 0

    # Pega TODAS as anotações do objeto de origem
    annotations = list(source_obj.listAnnotations())
    
    # LOG DE DIAGNÓSTICO:
    # log(f"DEBUG: Found {len(annotations)} total annotations on {source_obj.id}")

    if ann_filter:
        # Ajuste no filtro para aceitar tanto o nome da classe do objeto quanto do Wrapper
        annotations = [
            ann for ann in annotations 
            if ann.__class__.__name__ == ann_filter or 
               ann._obj.__class__.__name__ == ann_filter or
               ann.__class__.__name__ == ann_filter.replace("I", "Wrapper")
        ]

    if not annotations:
        # Se não achou nada que passe no filtro, avisamos
        # log(f"No annotations of type {ann_filter} found on source {source_obj.id}")
        return 0

    log(f"Found {len(annotations)} valid annotation(s) on source (ID: {source_obj.id})")

    link_map = {
        "Image": (ImageAnnotationLinkI, ImageI),
        "Plate": (PlateAnnotationLinkI, PlateI),
        "Well": (WellAnnotationLinkI, WellI)
    }

    LinkClass, ParentClass = link_map[target_type]
    target_ids = [obj.id for obj in target_objects]

    params = create_params(conn)
    existing_keys = set()
    for i in range(0, len(target_ids), BATCH_SIZE):
        batch_ids = target_ids[i:i + BATCH_SIZE]
        links = conn.getAnnotationLinks(target_type, batch_ids, params=params)
        for link in links:
            existing_keys.add(get_annotation_key(link))

    links_to_create = []
    for t_id in target_ids:
        for ann in annotations:
            key = f"{t_id}_{ann.id}"
            if key not in existing_keys:
                new_link = LinkClass()
                new_link.parent = ParentClass(t_id, False)
                new_link.child = ann._obj
                links_to_create.append(new_link)
                existing_keys.add(key)

    if links_to_create:
        update_service = conn.getUpdateService()
        for i in range(0, len(links_to_create), BATCH_SIZE):
            update_service.saveArray(links_to_create[i:i + BATCH_SIZE])
        log(f"Successfully linked {len(links_to_create)} annotations to targets.")
    else:
        log("Annotations already present on targets (skipping).")

    return len(links_to_create)

# =============================================================================
# Core Logic
# =============================================================================

def expand_annotations(conn: BlitzGateway, params: dict) -> int:
    dtype = params["Data_Type"].strip()
    ids = params["IDs"]
    ann_type = params["Annotation_Type"]
    source_level = params["Source_Level"]
    ann_filter = ANNOTATION_TYPES.get(ann_type)

    if dtype not in SUPPORTED_TYPES:
        raise ValueError(f"Unsupported type: {dtype}")

    log(f"Starting expansion: {dtype} IDs {ids} (Source: {source_level})")
    objects = list(conn.getObjects(dtype, ids))
    total_created = 0

    for obj in objects:
        # CASE 1: Full Screen Cascade (Screen -> Plate, Well, Image)
        if dtype == "Screen" and source_level == "Screen":
            log(f"Starting full cascade for Screen (ID: {obj.id})")
            plates = list(obj.listChildren())
            total_created += copy_annotations_to_targets(conn, obj, plates, "Plate", ann_filter)
            
            all_wells, all_images = [], []
            for p in plates:
                for w in p.listChildren():
                    all_wells.append(w)
                    all_images.extend([ws.getImage() for ws in w.listChildren() if ws.getImage()])
            
            total_created += copy_annotations_to_targets(conn, obj, all_wells, "Well", ann_filter)
            total_created += copy_annotations_to_targets(conn, obj, all_images, "Image", ann_filter)
            continue

        # CASE 2: Well-to-Image propagation (Specific metadata per well)
        if source_level == "Well":
            wells = []
            if dtype == "Well":
                wells = [obj]
            elif dtype == "Plate":
                wells = list(obj.listChildren())
            elif dtype == "Screen":
                for p in obj.listChildren(): 
                    wells.extend(list(p.listChildren()))

            for well in wells:
                images = [ws.getImage() for ws in well.listChildren() if ws.getImage()]
                if images:
                    log(f"Processing Well (ID: {well.id}): {len(images)} image(s)")
                    total_created += copy_annotations_to_targets(conn, well, images, "Image", ann_filter)
            continue

        # CASE 3: Standard Navigation (e.g., Plate -> Images, Project -> Images)
        source_obj = get_source_from_hierarchy(obj, dtype, source_level)
        if not source_obj:
            log(f"Warning: Could not find source level '{source_level}' for {dtype} {obj.id}")
            continue
        
        if dtype == "Plate" and source_level == "Plate":
            wells = list(obj.listChildren())
            if wells:
                log(f"Processing Plate ID: {obj.id} - Linking to {len(wells)} wells")
                total_created += copy_annotations_to_targets(conn, source_obj, wells, "Well", ann_filter)

        images = list(get_images_from_object(obj, dtype))
        if images:
            log(f"Processing {dtype} (ID: {obj.id}): {len(images)} image(s)")
            total_created += copy_annotations_to_targets(conn, source_obj, images, "Image", ann_filter)

    return total_created

# =============================================================================
# Main
# =============================================================================

def run_script():
    data_types = [rstring(t) for t in sorted(SUPPORTED_TYPES)]
    ann_types = [rstring(k) for k in ANNOTATION_TYPES.keys()]

    client = scripts.client(
        "Expand_Metadata.py",
        "Expand annotations from containers to images/sub-containers.",
        scripts.String("Data_Type", optional=False, grouping="1", values=data_types),
        scripts.List("IDs", optional=False, grouping="2").ofType(rlong(0)),
        scripts.String("Annotation_Type", optional=False, grouping="3", values=ann_types),
        scripts.String("Source_Level", optional=False, grouping="4", values=data_types),
        version="1.2",
        authors=["João V. S. Guerra", "Pablo W. A. Silva", "Rick H. Hokama"],
        institutions=["Brazilian Center for Research in Energy and Materials (CNPEM)"],
    )

    try:
        conn = BlitzGateway(client_obj=client)
        params = client.getInputs(unwrap=True)
        total = expand_annotations(conn, params)
        client.setOutput("Message", rstring(f"Success! {total} links created."))
    finally:
        client.closeSession()

if __name__ == "__main__":
    run_script()
