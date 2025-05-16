for f in *; do [ -f "$f" ] && { echo "File: $f"; tail -n 5 "$f"; echo "----"; }; done
