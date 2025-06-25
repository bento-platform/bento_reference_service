# Using RefGet

RefGet is a GA4GH-standard API "intended to be used in any scenario where full or partial access to reference sequence 
is required" which "enables access to reference sequences using an identifier derived from the sequence itself".

See the [RefGet v2.0.0 API specification page](https://samtools.github.io/hts-specs/refget.html) for more information.

The Bento Reference Service provides a RefGet v2 endpoint at `/sequence` as described in the specification linked above.

**Note:** The Bento Reference Service RefGet endpoint currently **DOES NOT** support circular contig access.
