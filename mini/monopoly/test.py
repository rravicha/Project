from PIL import Image

# Get the image paths
image_paths = [
    "image1.jpg",
    "image2.jpg",
    "image3.jpg"
]

# Create a new image with the same size as the largest image
new_image = Image.new("RGB", max([img.size for img in image_paths]))

# Paste the images onto the new image
for i, img_path in enumerate(image_paths):
    new_image.paste(Image.open(img_path), (0, i * img.size[1]))

# Save the new image
new_image.save("merged_images.jpg")