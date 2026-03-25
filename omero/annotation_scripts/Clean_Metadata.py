#!/usr/bin/env python
# -*- coding: utf-8 -*-

from omero import scripts
from omero.gateway import BlitzGateway
from omero.rtypes import rlong, rstring

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

SUPPORTED_TYPES = ["Project", "Dataset", "Screen", "Plate", "Well", "Image"]
BATCH_SIZE = 1000

# =============================================================================
# Deletion Logic
# =============================================================================

def delete_annotations(conn, target_objects, target_type, ann_filter):
    """
    Find and remove annotation links from target objects.
    """
    if not target_objects:
        return 0

    target_ids = [obj.id for obj in target_objects]
    links_to_delete = []

    # Map target types to full OMERO link class names to avoid NullPointerException
    link_class_map = {
        "Image": "omero.model.ImageAnnotationLink",
        "Plate": "omero.model.PlateAnnotationLink",
        "Well": "omero.model.WellAnnotationLink",
        "Dataset": "omero.model.DatasetAnnotationLink",
        "Project": "omero.model.ProjectAnnotationLink",
        "Screen": "omero.model.ScreenAnnotationLink",
    }

    full_link_class = link_class_map.get(target_type)

    for i in range(0, len(target_ids), BATCH_SIZE):
        batch_ids = target_ids[i:i + BATCH_SIZE]
        links = conn.getAnnotationLinks(target_type, batch_ids)

        for link in links:
            # Check if the annotation type matches the filter
            if not ann_filter or ann_filter in link.child.__class__.__name__:
                links_to_delete.append(link.id)

    if links_to_delete:
        print(f"   -> Found {len(links_to_delete)} links on {target_type}. Deleting...")
        # Using full class path prevents the 'graph-fail' Java error
        conn.deleteObjects(full_link_class, links_to_delete, wait=True)

    return len(links_to_delete)

# =============================================================================
# Main Execution Flow
# =============================================================================

def run_script():
    client = scripts.client(
        "Clean_Metadata.py",
        "CLEANUP: Remove annotation links from containers and their children.",
        scripts.String("Data_Type", optional=False, grouping="1", values=[rstring(t) for t in SUPPORTED_TYPES]),
        scripts.List("IDs", optional=False, grouping="1.1").ofType(rlong(0)),
        scripts.String("Annotation_Type", optional=False, grouping="1.2", values=[rstring(k) for k in sorted(ANNOTATION_TYPES.keys())]),
        scripts.Bool("Include_Children", optional=False, grouping="1.3"),
        version="1.0",
        authors=["Rick H. Hokama", "João V. S. Guerra"],
        institutions=["Brazilian Center for Research in Energy and Materials (CNPEM)"]
    )

    try:
        conn = BlitzGateway(client_obj=client)
        params = client.getInputs(unwrap=True)
        dtype, ids = params["Data_Type"], params["IDs"]
        ann_filter = ANNOTATION_TYPES.get(params["Annotation_Type"])

        objects = list(conn.getObjects(dtype, ids))
        total_deleted = 0

        for obj in objects:
            print(f"Processing {dtype} (ID: {obj.id})")

            # 1. Clear selected parent object
            total_deleted += delete_annotations(conn, [obj], dtype, ann_filter)

            # 2. Clear children if requested

            # set to True as default in case value passed on prompt is null or not boolean
            include_children = bool(params.get("Include_Children", True))

            if include_children:
                if dtype == "Screen":
                    plates = list(obj.listChildren())
                    wells = [w for p in plates for w in p.listChildren()]
                    imgs = [ws.getImage() for w in wells for ws in w.listChildren() if ws.getImage()]

                    total_deleted += delete_annotations(conn, plates, "Plate", ann_filter)
                    total_deleted += delete_annotations(conn, wells, "Well", ann_filter)
                    total_deleted += delete_annotations(conn, imgs, "Image", ann_filter)

                elif dtype == "Plate":
                    wells = list(obj.listChildren())
                    imgs = [ws.getImage() for w in wells for ws in w.listChildren() if ws.getImage()]

                    total_deleted += delete_annotations(conn, wells, "Well", ann_filter)
                    total_deleted += delete_annotations(conn, imgs, "Image", ann_filter)

                elif dtype in ["Project", "Dataset"]:
                    imgs = list(obj.listAllObjects("Image"))
                    total_deleted += delete_annotations(conn, imgs, "Image", ann_filter)

                elif dtype == "Well":
                    imgs = [ws.getImage() for ws in obj.listChildren() if ws.getImage()]
                    total_deleted += delete_annotations(conn, imgs, "Image", ann_filter)

        print(f"Cleanup complete. Total of {total_deleted} links removed.")
        client.setOutput("Message", rstring(f"Success: {total_deleted} links removed."))

    finally:
        client.closeSession()

if __name__ == "__main__":
    run_script()
